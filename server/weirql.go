package main

// WeirQL v0 grammar:
//
//	query    = IDENT "(" selector [ "[" DURATION "]" ] [ "," NUMBER ] ")" [ "by" "(" IDENT { "," IDENT } ")" ]
//	selector = "{" [ matcher { "," matcher } ] "}"
//	matcher  = IDENT ( "=" | "!=" | "=~" | "!~" ) STRING

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Matcher struct {
	Label string
	Op    string
	Value string
	re    *regexp.Regexp
}

func (m Matcher) Matches(v string) bool {
	switch m.Op {
	case "=":
		return v == m.Value
	case "!=":
		return v != m.Value
	case "=~":
		return m.re.MatchString(v)
	case "!~":
		return !m.re.MatchString(v)
	}
	return false
}

type Query struct {
	Func     string
	Matchers []Matcher
	Range    time.Duration // 0 if not given
	Param    *float64
	By       []string
}

type tokenKind int

const (
	tEOF tokenKind = iota
	tIdent
	tString
	tNumber
	tDuration
	tPunct
)

type token struct {
	kind tokenKind
	text string
	pos  int
}

func lex(src string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(src) {
		c := rune(src[i])
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '"':
			j := i + 1
			var b strings.Builder
			for j < len(src) && src[j] != '"' {
				if src[j] == '\\' && j+1 < len(src) {
					j++
				}
				b.WriteByte(src[j])
				j++
			}
			if j >= len(src) {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			toks = append(toks, token{tString, b.String(), i})
			i = j + 1
		case unicode.IsDigit(c) || c == '.':
			j := i
			for j < len(src) && (unicode.IsDigit(rune(src[j])) || src[j] == '.') {
				j++
			}
			if j < len(src) && strings.ContainsRune("smhd", rune(src[j])) {
				toks = append(toks, token{tDuration, src[i : j+1], i})
				i = j + 1
			} else {
				toks = append(toks, token{tNumber, src[i:j], i})
				i = j
			}
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(src) && (unicode.IsLetter(rune(src[j])) || unicode.IsDigit(rune(src[j])) || src[j] == '_') {
				j++
			}
			toks = append(toks, token{tIdent, src[i:j], i})
			i = j
		default:
			if i+1 < len(src) {
				two := src[i : i+2]
				if two == "!=" || two == "=~" || two == "!~" {
					toks = append(toks, token{tPunct, two, i})
					i += 2
					continue
				}
			}
			if !strings.ContainsRune("{}()[],=", c) {
				return nil, fmt.Errorf("unexpected %q at %d", c, i)
			}
			toks = append(toks, token{tPunct, string(c), i})
			i++
		}
	}
	return append(toks, token{tEOF, "", len(src)}), nil
}

type parser struct {
	toks []token
	i    int
}

func (p *parser) peek() token { return p.toks[p.i] }
func (p *parser) next() token { t := p.toks[p.i]; p.i++; return t }

func (p *parser) expect(kind tokenKind, text string) (token, error) {
	t := p.next()
	if t.kind != kind || (text != "" && t.text != text) {
		want := text
		if want == "" {
			want = [...]string{"end", "identifier", "string", "number", "duration", "punctuation"}[kind]
		}
		got := t.text
		if t.kind == tEOF {
			got = "end of query"
		}
		return t, fmt.Errorf("expected %s at %d, got %q", want, t.pos, got)
	}
	return t, nil
}

func Parse(src string) (*Query, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q := &Query{}

	fn, err := p.expect(tIdent, "")
	if err != nil {
		return nil, err
	}
	q.Func = fn.text
	if _, err := p.expect(tPunct, "("); err != nil {
		return nil, err
	}
	if q.Matchers, err = p.selector(); err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tPunct && t.text == "[" {
		p.next()
		d, err := p.expect(tDuration, "")
		if err != nil {
			return nil, err
		}
		if q.Range, err = parseDuration(d.text); err != nil {
			return nil, err
		}
		if _, err := p.expect(tPunct, "]"); err != nil {
			return nil, err
		}
	}
	if t := p.peek(); t.kind == tPunct && t.text == "," {
		p.next()
		n, err := p.expect(tNumber, "")
		if err != nil {
			return nil, err
		}
		v, err := strconv.ParseFloat(n.text, 64)
		if err != nil {
			return nil, err
		}
		q.Param = &v
	}
	if _, err := p.expect(tPunct, ")"); err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tIdent && t.text == "by" {
		p.next()
		if _, err := p.expect(tPunct, "("); err != nil {
			return nil, err
		}
		for {
			l, err := p.expect(tIdent, "")
			if err != nil {
				return nil, err
			}
			q.By = append(q.By, l.text)
			if t := p.next(); t.text == ")" {
				break
			} else if t.text != "," {
				return nil, fmt.Errorf("expected , or ) at %d", t.pos)
			}
		}
	}
	if _, err := p.expect(tEOF, ""); err != nil {
		return nil, err
	}
	return q, nil
}

func (p *parser) selector() ([]Matcher, error) {
	if _, err := p.expect(tPunct, "{"); err != nil {
		return nil, err
	}
	var ms []Matcher
	if t := p.peek(); t.kind == tPunct && t.text == "}" {
		p.next()
		return ms, nil
	}
	for {
		l, err := p.expect(tIdent, "")
		if err != nil {
			return nil, err
		}
		op := p.next()
		if op.kind != tPunct || !strings.Contains(" = != =~ !~ ", " "+op.text+" ") {
			return nil, fmt.Errorf("expected =, !=, =~ or !~ at %d", op.pos)
		}
		v, err := p.expect(tString, "")
		if err != nil {
			return nil, err
		}
		m := Matcher{Label: l.text, Op: op.text, Value: v.text}
		if op.text == "=~" || op.text == "!~" {
			if m.re, err = regexp.Compile("^(?:" + v.text + ")$"); err != nil {
				return nil, fmt.Errorf("bad regex %q: %w", v.text, err)
			}
		}
		ms = append(ms, m)
		t := p.next()
		if t.text == "}" {
			return ms, nil
		}
		if t.text != "," {
			return nil, fmt.Errorf("expected , or } at %d", t.pos)
		}
	}
}

func parseDuration(s string) (time.Duration, error) {
	unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour}[s[len(s)-1]]
	n, err := strconv.ParseFloat(s[:len(s)-1], 64)
	if err != nil {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	return time.Duration(n * float64(unit)), nil
}
