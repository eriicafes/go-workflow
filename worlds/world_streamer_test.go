package worlds

import "testing"

func TestUserStreamNameMatchesTheSDK(t *testing.T) {
	// Expected names join strm_, the run ID without wrun_, _user, and the
	// name in unpadded base64url.
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
