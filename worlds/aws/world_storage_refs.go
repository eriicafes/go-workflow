package awsworld

import (
	"context"
	"encoding/json"
	"slices"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// dataFields are the run and step attributes that can outgrow an item. Each
// value is also held by the event that set it, so a view whose item would
// exceed DynamoDB's item limit stores the event's slot as <field>Ref instead
// of the value, and reads resolve it from that event.
var dataFields = []string{"input", "output", "error"}

// dataRefs maps a data field to the slot of the event holding its value.
type dataRefs map[string]uint64

// set records that slot holds field's value, or clears it for an empty value.
func (refs *dataRefs) set(field string, slot uint64, value []byte) {
	if len(value) == 0 {
		delete(*refs, field)
		return
	}
	if *refs == nil {
		*refs = dataRefs{}
	}
	(*refs)[field] = slot
}

// storeData adds values to item inline, moving the largest to references
// while item is over the item limit. A value not loaded, because its item
// held a reference, keeps that reference.
func storeData(item map[string]ddb.AttributeValue, values map[string][]byte, refs dataRefs) {
	for _, field := range dataFields {
		if value := values[field]; value != nil {
			setBytes(item, field, value)
		} else if slot := refs[field]; slot != 0 {
			item[field+"Ref"] = avn(int64(slot))
		}
	}
	for itemSize(item) > maxItemBytes {
		largest := ""
		for _, field := range dataFields {
			if _, inline := item[field]; inline && refs[field] != 0 && (largest == "" || len(values[field]) > len(values[largest])) {
				largest = field
			}
		}
		if largest == "" {
			return
		}
		delete(item, largest)
		item[largest+"Ref"] = avn(int64(refs[largest]))
	}
}

// readRefs returns the references item holds in place of data values.
func readRefs(item map[string]ddb.AttributeValue) dataRefs {
	refs := dataRefs{}
	for _, field := range dataFields {
		if slot := readInt(item, field+"Ref"); slot > 0 {
			refs[field] = uint64(slot)
		}
	}
	return refs
}

// resolveData reads the referenced values of a run or step from their events.
// fields selects the values the caller returns.
func (w *World) resolveData(ctx context.Context, runID worlds.RunID, refs dataRefs, values map[string]*[]byte, fields ...string) error {
	var keys []map[string]ddb.AttributeValue
	for _, field := range fields {
		if slot := refs[field]; slot != 0 && *values[field] == nil {
			keys = append(keys, eventKey(string(runID), slot))
		}
	}
	if len(keys) == 0 {
		return nil
	}
	items, err := w.items(ctx, keys...)
	if err != nil {
		return err
	}
	for _, field := range fields {
		slot := refs[field]
		if slot == 0 || *values[field] != nil {
			continue
		}
		if item, ok := items[itemID(eventKey(string(runID), slot))]; ok {
			*values[field] = eventValue(eventFromItem(item), field)
		}
	}
	return nil
}

// eventValue decodes field from the event that set it.
func eventValue(event worlds.Event, field string) []byte {
	switch event.EventType {
	case worlds.EventTypeRunCreated:
		var data worlds.DataRunCreated
		_ = json.Unmarshal(event.Data, &data)
		return data.Input
	case worlds.EventTypeRunCompleted:
		return worlds.DecodeRunCompleted(event.Data)
	case worlds.EventTypeRunFailed:
		return worlds.DecodeRunFailed(event.Data)
	case worlds.EventTypeStepCreated:
		var data worlds.DataStepCreated
		_ = json.Unmarshal(event.Data, &data)
		return data.Input
	}
	var result worlds.DataStepResult
	_ = json.Unmarshal(event.Data, &result)
	if field == "output" {
		return result.Result
	}
	return result.Error
}

// resolveRun reads run's referenced values that resolve returns.
func (w *World) resolveRun(ctx context.Context, view runView, resolve worlds.ResolveData) (worlds.Run, error) {
	run := view.Run
	fields := slices.Clone(dataFields)
	if resolve == worlds.ResolveDataNone {
		fields = []string{"error"}
	}
	err := w.resolveData(ctx, run.RunID, view.refs, map[string]*[]byte{"input": &run.Input, "output": &run.Output, "error": &run.Error}, fields...)
	return stripRun(run, resolve), err
}

// resolveStep reads step's referenced values that resolve returns.
func (w *World) resolveStep(ctx context.Context, view stepView, resolve worlds.ResolveData) (worlds.Step, error) {
	step := view.Step
	fields := slices.Clone(dataFields)
	if resolve == worlds.ResolveDataNone {
		fields = []string{"error"}
	}
	err := w.resolveData(ctx, step.RunID, view.refs, map[string]*[]byte{"input": &step.Input, "output": &step.Output, "error": &step.Error}, fields...)
	return stripStep(step, resolve), err
}
