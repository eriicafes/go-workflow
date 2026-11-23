/** The runner shown in the home page replay demo. Frame line numbers in ReplayDemo refer to it. */
export const replayDemoCode = `func GenerateReport(ctx context.Context, in workflows.GenerateReport) (Result, error) {
	sources, err := workflow.Step(ctx, findSources, in.Topic)
	if err != nil {
		return Result{}, err
	}

	// Suspends the run. No process waits for it.
	if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
		return Result{}, err
	}

	return workflow.Step(ctx, summarize, sources)
}`;
