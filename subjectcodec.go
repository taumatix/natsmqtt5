package natsmqtt5

import "github.com/taumatix/natsmqtt5/topic"

// subjectCodec maps Topic Names and Filters to NATS subjects and back. With
// escape set (Options.EscapeNATSWildcards) it uses the escaping variants, which
// admit '*', '>' and whitespace in a topic; without it, the plain mapping that
// matches nats-server's own.
type subjectCodec struct{ escape bool }

func (k subjectCodec) validateName(n string) error {
	if k.escape {
		return topic.ValidateNameEscaped(n)
	}
	return topic.ValidateName(n)
}

func (k subjectCodec) validateFilter(f string) error {
	if k.escape {
		return topic.ValidateFilterEscaped(f)
	}
	return topic.ValidateFilter(f)
}

func (k subjectCodec) nameToSubject(n string) (string, error) {
	if k.escape {
		return topic.NameToSubjectEscaped(n)
	}
	return topic.NameToSubject(n)
}

func (k subjectCodec) filterToSubject(f string) (string, error) {
	if k.escape {
		return topic.FilterToSubjectEscaped(f)
	}
	return topic.FilterToSubject(f)
}

func (k subjectCodec) subjectToName(s string) string {
	if k.escape {
		return topic.SubjectToNameEscaped(s)
	}
	return topic.SubjectToName(s)
}
