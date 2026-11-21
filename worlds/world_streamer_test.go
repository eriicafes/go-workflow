package worlds

import "testing"

func TestUserStreamNameMatchesTheSDK(t *testing.T) {
	// Expected names are from the Workflow SDK's getWorkflowRunStreamId.
	for _, test := range []struct{ name, want string }{
		{"", "strm_01K_user"},
		{"progress", "strm_01K_user_cHJvZ3Jlc3M"},
		{"~~~ü", "strm_01K_user_fn5-w7w"},
	} {
		if got := UserStreamName("wrun_01K", test.name); got != test.want {
			t.Errorf("UserStreamName(%q) = %q, want %q", test.name, got, test.want)
		}
	}
}
