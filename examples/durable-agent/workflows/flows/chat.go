// Package flows holds the example's workflow runners.
package flows

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/durable-agent/workflows"
	"github.com/eriicafes/go-workflow/workflowai"
)

// Chat answers one chat turn with a flight booking agent. Searching runs as a
// durable step, and booking also waits for the user's approval, which arrives
// as the next turn's messages.
func Chat(ctx context.Context, input workflows.Chat) (workflows.ChatResult, error) {
	model, err := LanguageModel()
	if err != nil {
		return workflows.ChatResult{}, err
	}
	messages, err := workflowai.ConvertToModelMessages(ctx, input.Messages)
	if err != nil {
		return workflows.ChatResult{}, err
	}

	agent := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{
		ID:    "flight-agent",
		Model: model,
		Instructions: "You are a flight booking assistant. Search for flights before booking one, " +
			"and confirm the flight and passenger name with the user before booking. Keep answers short.",
		Tools: workflowai.ToolSet{
			"searchFlights": {
				Description: "Search for available flights between two airports on a date.",
				Execute:     workflowai.ExecuteStep(searchFlights),
			},
			"bookFlight": {
				Description:   "Book a flight for a passenger. Search for flights first.",
				NeedsApproval: workflowai.NeedsApprovalAlways,
				Execute:       workflowai.ExecuteStep(bookFlight),
			},
		},
		StopWhen:           []workflowai.StopCondition{workflowai.IsStepCount(10)},
		ToolApprovalSecret: toolApprovalSecret(),
	})

	result, err := agent.Stream(ctx, workflowai.CallOptions{
		Messages: messages,
	})
	return workflows.ChatResult{Messages: result.Messages}, err
}

type flightSearch struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	Date        string `json:"date"`
}

type flight struct {
	FlightID  string `json:"flightId"`
	Airline   string `json:"airline"`
	Departure string `json:"departure"`
	Arrival   string `json:"arrival"`
	PriceUSD  int    `json:"priceUsd"`
}

var airlines = []string{"Skyward", "Blue Heron", "Northwind"}

// searchFlights returns made-up flights that are the same for the same
// search, after a delay like a real flight API's.
func searchFlights(ctx context.Context, search flightSearch) ([]flight, error) {
	select {
	case <-time.After(1500 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	hash := fnv.New32a()
	fmt.Fprintf(hash, "%s|%s|%s", strings.ToUpper(search.Origin), strings.ToUpper(search.Destination), search.Date)
	seed := int(hash.Sum32())
	flights := make([]flight, len(airlines))
	for i, airline := range airlines {
		hour := 6 + (seed/(i+1))%14
		flights[i] = flight{
			FlightID:  fmt.Sprintf("%s%d", airline[:2], 100+(seed/(i+7))%900),
			Airline:   airline,
			Departure: fmt.Sprintf("%s %02d:%02d", search.Date, hour, (seed/(i+3))%4*15),
			Arrival:   fmt.Sprintf("%s %02d:%02d", search.Date, hour+2+i, (seed/(i+5))%4*15),
			PriceUSD:  180 + (seed/(i+11))%400,
		}
	}
	return flights, nil
}

type booking struct {
	FlightID      string `json:"flightId"`
	PassengerName string `json:"passengerName"`
}

type confirmation struct {
	ConfirmationCode string `json:"confirmationCode"`
	FlightID         string `json:"flightId"`
	PassengerName    string `json:"passengerName"`
}

// bookFlight books a flight. The confirmation code comes from the step ID,
// which stays the same when the step is retried, so a real booking API could
// use it as an idempotency key.
func bookFlight(ctx context.Context, input booking) (confirmation, error) {
	stepID := workflow.StepID(ctx)
	code := strings.ToUpper(stepID[len(stepID)-6:])
	return confirmation{ConfirmationCode: code, FlightID: input.FlightID, PassengerName: input.PassengerName}, nil
}
