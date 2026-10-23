package topic

import "strings"

// The conversion below reproduces nats-server's mqttToNATSSubjectConversion
// and natsSubjectToMQTTTopic (server/mqtt.go, v2.14.6). Keeping it identical
// is the whole point: the same MQTT topic must reach the same NATS subject
// whichever of the two brokers publishes it.
//
// The rules, in the order they are applied to each byte of the topic:
//
//	'/' at the start of the topic       -> "/."
//	'/' at the end, or before another '/' -> "./"
//	'/' anywhere else                   -> "."
//	'.'                                 -> "//"
//	'+' (filters only)                  -> "*"
//	'#' (filters only)                  -> ">"
//
// and a trailing '.' in the result gets a '/' appended. So "foo/bar" becomes
// "foo.bar", "foo//bar" becomes "foo./.bar", "/foo" becomes "/.foo" and
// "a.b" becomes "a//b".

// NameToSubject converts a Topic Name to a NATS subject. Wildcards are
// rejected, since a PUBLISH must not carry them [MQTT-3.3.2-2].
func NameToSubject(name string) (string, error) {
	if err := ValidateName(name); err != nil {
		return "", err
	}
	return convert(name, false, false), nil
}

// NameToSubjectEscaped is NameToSubject for a topic that may contain the
// characters a NATS subject cannot carry. Each is written as '/' and a letter,
// a sequence the nats-server mapping never produces (it writes '/' only before
// '.', before another '/', or at the end): '*' is "/s", '>' is "/g", space is
// "/p", tab "/t", line feed "/n", carriage return "/r", form feed "/f" and DEL
// "/d". A topic with none of them maps to exactly the subject NameToSubject
// gives it; one with any does not, which is the interoperability the option
// trades away.
func NameToSubjectEscaped(name string) (string, error) {
	if err := ValidateNameEscaped(name); err != nil {
		return "", err
	}
	return convert(name, false, true), nil
}

// FilterToSubject converts a Topic Filter to a NATS subject, translating '+'
// to the NATS single-token wildcard '*' and '#' to the multi-token wildcard
// '>'.
//
// For a shared filter the ShareName is stripped first: it selects the NATS
// queue group rather than forming part of the subject (MQTT-5.0 §4.8.2).
// Callers get the ShareName from SplitShared.
func FilterToSubject(filter string) (string, error) {
	if err := ValidateFilter(filter); err != nil {
		return "", err
	}
	_, inner, err := SplitShared(filter)
	if err != nil {
		return "", err
	}
	return convert(inner, true, false), nil
}

// FilterToSubjectEscaped is FilterToSubject for a filter whose levels may
// contain the characters NameToSubjectEscaped escapes. A '+' or '#' that is a
// wildcard still becomes '*' or '>'; a '*' or '>' inside a level is an
// ordinary character and is escaped.
func FilterToSubjectEscaped(filter string) (string, error) {
	if err := ValidateFilterEscaped(filter); err != nil {
		return "", err
	}
	_, inner, err := SplitShared(filter)
	if err != nil {
		return "", err
	}
	return convert(inner, true, true), nil
}

// SubjectToName converts a NATS subject produced by NameToSubject back to the
// MQTT Topic Name it came from. The conversion is exact: for any name that
// NameToSubject accepts, SubjectToName(NameToSubject(name)) == name.
func SubjectToName(subject string) string { return subjectToName(subject, false) }

// SubjectToNameEscaped is SubjectToName for a subject NameToSubjectEscaped
// produced: for any name it accepts, SubjectToNameEscaped(NameToSubjectEscaped(name)) == name.
func SubjectToNameEscaped(subject string) string { return subjectToName(subject, true) }

// escapes maps the letter after '/' to the character it stands for.
var escapes = map[byte]byte{'s': '*', 'g': '>', 'p': ' ', 't': '\t', 'n': '\n', 'r': '\r', 'f': '\f', 'd': 0x7f}

// escaped is the letter that follows '/' for a character, or 0.
func escaped(c byte) byte {
	switch c {
	case '*':
		return 's'
	case '>':
		return 'g'
	case ' ':
		return 'p'
	case '\t':
		return 't'
	case '\n':
		return 'n'
	case '\r':
		return 'r'
	case '\f':
		return 'f'
	case 0x7f:
		return 'd'
	}
	return 0
}

func subjectToName(subject string, esc bool) string {
	var b strings.Builder
	b.Grow(len(subject))
	end := len(subject) - 1
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case '/':
			// "/." encodes a literal '/', "//" encodes a literal '.', and a
			// trailing '/' is padding that carries no character.
			if i < end {
				switch subject[i+1] {
				case '.':
					b.WriteByte('/')
					i++
				case '/':
					b.WriteByte('.')
					i++
				default:
					if c, ok := escapes[subject[i+1]]; ok && esc {
						b.WriteByte(c)
						i++
					}
				}
			}
		case '.':
			b.WriteByte('/')
		default:
			b.WriteByte(subject[i])
		}
	}
	return b.String()
}

func convert(mt string, wildcards, escape bool) string {
	var b strings.Builder
	b.Grow(len(mt) + 8)
	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch c := mt[i]; c {
		case '/':
			switch {
			case i == 0 || lastByte(&b) == '.':
				b.WriteString("/.")
			case i == end || mt[i+1] == '/':
				b.WriteString("./")
			default:
				b.WriteByte('.')
			}
		case '.':
			b.WriteString("//")
		case '*', '>', ' ', '\t', '\n', '\r', '\f', 0x7f:
			if l := escaped(c); escape && l != 0 {
				b.WriteByte('/')
				b.WriteByte(l)
			} else {
				b.WriteByte(c)
			}
		case '+':
			if wildcards {
				b.WriteByte('*')
			} else {
				b.WriteByte(c)
			}
		case '#':
			if wildcards {
				b.WriteByte('>')
			} else {
				b.WriteByte(c)
			}
		default:
			b.WriteByte(c)
		}
	}
	out := b.String()
	if strings.HasSuffix(out, ".") {
		out += "/"
	}
	return out
}

func lastByte(b *strings.Builder) byte {
	s := b.String()
	if s == "" {
		return 0
	}
	return s[len(s)-1]
}

// ParentSubject handles the one place where NATS and MQTT wildcards disagree.
//
// MQTT's '#' "represents the parent and any number of child levels", so
// "sport/#" matches the topic "sport" itself (MQTT-5.0 §4.7.1.2). NATS's '>'
// requires at least one token, so "sport.>" does not match "sport". A
// subscriber therefore needs a second NATS subscription on the parent subject.
//
// ParentSubject returns that parent and true when subject ends in ".>" with
// something before it. A bare ">" needs no companion: it already matches every
// subject. This mirrors nats-server's mqttNeedSubForLevelUp.
func ParentSubject(subject string) (string, bool) {
	if len(subject) < 3 || !strings.HasSuffix(subject, ".>") {
		return "", false
	}
	return subject[:len(subject)-2], true
}

// Prefix namespaces a converted subject under a NATS subject prefix, so that
// MQTT traffic occupies a known part of the subject space and cannot collide
// with NATS system subjects. An empty prefix returns the subject unchanged.
func Prefix(prefix, subject string) string {
	if prefix == "" {
		return subject
	}
	return prefix + "." + subject
}

// TrimPrefix removes a prefix added by Prefix. It reports false if subject is
// not under prefix.
func TrimPrefix(prefix, subject string) (string, bool) {
	if prefix == "" {
		return subject, true
	}
	p := prefix + "."
	if !strings.HasPrefix(subject, p) {
		return "", false
	}
	return subject[len(p):], true
}
