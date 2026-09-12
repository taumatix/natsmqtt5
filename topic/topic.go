// Package topic implements MQTT Topic Name and Topic Filter handling, and the
// mapping between MQTT topics and NATS subjects.
//
// The mapping is not invented here. It is the one nats-server uses for its
// built-in MQTT 3.1.1 support (server/mqtt.go, mqttToNATSSubjectConversion),
// reproduced so that a topic published through this broker lands on exactly
// the same NATS subject as the same topic published through nats-server. That
// matters for anyone running both, and for NATS-native subscribers who address
// the subject namespace directly.
//
// Section numbers refer to the OASIS MQTT v5.0 Standard of 07 March 2019.
package topic

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalid reports a Topic Name or Topic Filter that MQTT or the NATS
// subject mapping does not permit.
var ErrInvalid = errors.New("mqtt topic: invalid")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// MaxLength is the ceiling MQTT places on a Topic Name or Topic Filter, which
// is a UTF-8 Encoded String [MQTT-4.7.3-3].
const MaxLength = 65535

// SharedPrefix marks a Shared Subscription Topic Filter (MQTT-5.0 §4.8.2).
const SharedPrefix = "$share/"

// ValidateName checks a Topic Name, as carried in a PUBLISH
// (MQTT-5.0 §4.7.3). Wildcards are forbidden [MQTT-4.7.0-1].
func ValidateName(name string) error {
	if err := validateCommon(name); err != nil {
		return err
	}
	if strings.ContainsAny(name, "+#") {
		return invalid("Topic Name %q contains a wildcard [MQTT-3.3.2-2]", name)
	}
	return nil
}

// ValidateFilter checks a Topic Filter, as carried in SUBSCRIBE and
// UNSUBSCRIBE (MQTT-5.0 §4.7.1, §4.8.2). Shared subscription filters are
// accepted and their {filter} part validated.
func ValidateFilter(filter string) error {
	if err := validateCommon(filter); err != nil {
		return err
	}
	if strings.HasPrefix(filter, SharedPrefix) {
		share, rest, err := SplitShared(filter)
		if err != nil {
			return err
		}
		_ = share
		return validateFilterLevels(rest)
	}
	return validateFilterLevels(filter)
}

func validateCommon(s string) error {
	// "All Topic Names and Topic Filters MUST be at least one character long"
	// [MQTT-4.7.3-1].
	if s == "" {
		return invalid("empty [MQTT-4.7.3-1]")
	}
	if len(s) > MaxLength {
		return invalid("%d bytes, exceeds the %d-byte limit [MQTT-4.7.3-3]", len(s), MaxLength)
	}
	// "Topic Names and Topic Filters MUST NOT include the null character"
	// [MQTT-4.7.3-2].
	if strings.ContainsRune(s, 0) {
		return invalid("contains U+0000 [MQTT-4.7.3-2]")
	}
	// The NATS subject mapping cannot carry these: whitespace would split the
	// NATS protocol control line, and DEL is a pivot marker in the server's
	// subject tree. nats-server rejects them for the same reason.
	if i := strings.IndexAny(s, " \t\n\r\f\x7f"); i >= 0 {
		return invalid("contains %q at offset %d, which cannot be carried in a NATS subject", s[i], i)
	}
	// '*' and '>' are ordinary characters in MQTT but wildcards in NATS, and
	// the subject mapping does not escape them. nats-server lets them through,
	// so an MQTT subscription to "a/*/b" there quietly becomes a NATS wildcard
	// and receives topics it must not see. We reject them instead: a loud
	// error beats silent mis-delivery. Escaping them would fix the topic but
	// break subject-level interop with nats-server for every topic that uses
	// one, so it is deferred to a later version behind an option.
	if i := strings.IndexAny(s, "*>"); i >= 0 {
		return invalid("contains %q at offset %d, which this broker cannot carry in a NATS subject unambiguously", s[i], i)
	}
	return nil
}

// validateFilterLevels enforces the wildcard placement rules of
// MQTT-5.0 §4.7.1.2 and §4.7.1.3.
func validateFilterLevels(filter string) error {
	if filter == "" {
		return invalid("shared subscription has an empty filter [MQTT-4.8.2-2]")
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		// A '+' is legal only when it occupies the whole level
		// [MQTT-4.7.1-2].
		if level != "+" && strings.Contains(level, "+") {
			return invalid("%q: '+' must occupy an entire level [MQTT-4.7.1-2]", filter)
		}
		if strings.Contains(level, "#") {
			// '#' must stand alone and be the last character
			// [MQTT-4.7.1-1].
			if level != "#" {
				return invalid("%q: '#' must occupy an entire level [MQTT-4.7.1-1]", filter)
			}
			if i != len(levels)-1 {
				return invalid("%q: '#' must be the last level [MQTT-4.7.1-1]", filter)
			}
		}
	}
	return nil
}

// SplitShared splits a Shared Subscription Topic Filter into its ShareName and
// the filter it shares (MQTT-5.0 §4.8.2). It returns ok=false, with the filter
// unchanged, when f is not a shared filter.
//
// A filter that starts with "$share/" but is malformed returns an error, since
// [MQTT-4.8.2-1] and [MQTT-4.8.2-2] make that a rejectable subscription rather
// than an ordinary topic.
func SplitShared(f string) (share, filter string, err error) {
	if !strings.HasPrefix(f, SharedPrefix) {
		return "", f, nil
	}
	rest := f[len(SharedPrefix):]
	slash := strings.IndexByte(rest, '/')
	switch {
	case slash < 0:
		return "", "", invalid("%q has no '/' after the ShareName [MQTT-4.8.2-2]", f)
	case slash == 0:
		return "", "", invalid("%q has an empty ShareName [MQTT-4.8.2-1]", f)
	}
	share, filter = rest[:slash], rest[slash+1:]
	// "{ShareName} is a character string that does not include '/', '+' or
	// '#'" [MQTT-4.8.2-2]. The '/' is already excluded by the split.
	if strings.ContainsAny(share, "+#") {
		return "", "", invalid("ShareName %q contains a wildcard [MQTT-4.8.2-2]", share)
	}
	if filter == "" {
		return "", "", invalid("%q has an empty filter [MQTT-4.8.2-2]", f)
	}
	return share, filter, nil
}

// IsShared reports whether f is a Shared Subscription Topic Filter.
func IsShared(f string) bool { return strings.HasPrefix(f, SharedPrefix) }

// Match reports whether the Topic Filter matches the Topic Name
// (MQTT-5.0 §4.7). A shared filter matches on its {filter} part alone: "the
// $share and {ShareName} portions of the Topic Filter are not taken into
// account when matching against publications" (MQTT-5.0 §4.8.2).
//
// It implements [MQTT-4.7.2-1]: a filter beginning with a wildcard never
// matches a topic beginning with '$'.
func Match(filter, name string) bool {
	if IsShared(filter) {
		_, inner, err := SplitShared(filter)
		if err != nil {
			return false
		}
		filter = inner
	}
	if filter == "" || name == "" {
		return false
	}
	// "The Server MUST NOT match Topic Filters starting with a wildcard
	// character (# or +) with Topic Names beginning with a $ character"
	// [MQTT-4.7.2-1].
	if name[0] == '$' && (filter[0] == '#' || filter[0] == '+') {
		return false
	}

	f := strings.Split(filter, "/")
	n := strings.Split(name, "/")
	for i, level := range f {
		if level == "#" {
			// '#' matches the parent level and any number of child levels, so
			// "sport/#" matches "sport" (MQTT-5.0 §4.7.1.2). It is always the
			// last level, so everything left in n is absorbed.
			return true
		}
		if i >= len(n) {
			return false
		}
		if level != "+" && level != n[i] {
			return false
		}
	}
	return len(f) == len(n)
}
