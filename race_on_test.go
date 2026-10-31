//go:build race

package natsmqtt5

// raceEnabled is true under -race, which makes CPU-time bounds meaningless (about ten times slower).
const raceEnabled = true
