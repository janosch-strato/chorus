/*
 * Copyright © 2026 STRATO GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package switches

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Bool(t *testing.T) {
	r := require.New(t)
	applied := []bool{}
	s := NewBool("test-bool", true, func(v bool) error { applied = append(applied, v); return nil })

	r.Equal("test-bool", s.Name())
	r.Equal("enabled, disabled", s.Syntax())
	r.True(s.Get(), "the default it was made with")
	r.Equal(Enabled, s.GetString())

	r.NoError(s.SetString(Disabled))
	r.False(s.Get())
	r.Equal(Disabled, s.GetString())
	r.Equal([]bool{false}, applied, "the change is applied")

	r.NoError(s.SetString(Disabled))
	r.Equal([]bool{false}, applied, "setting the same value again is no change")

	r.NoError(s.SetString(""), "the empty value is the default")
	r.True(s.Get())
	r.Equal([]bool{false, true}, applied)

	r.NoError(s.Set(false), "a config key that is a bool needs no spelling")
	r.False(s.Get())

	r.ErrorIs(s.SetString("maybe"), ErrInvalidValue)
	r.False(s.Get(), "a refused value leaves it alone")
	r.Len(applied, 3, "and applies nothing")
}

func Test_Enum(t *testing.T) {
	r := require.New(t)
	applied := []string{}
	s := NewEnum("test-enum", func(v string) error { applied = append(applied, v); return nil })
	first := s.NewValue("first")
	second := s.NewValue("second")
	third := s.NewValue("third")
	r.EqualValues(0, first, "the values are the positions they were added in")
	r.EqualValues(1, second)
	r.EqualValues(2, third)

	r.Equal("first, second, third", s.Syntax())
	r.Equal(first, s.Get(), "the value added first is the default")
	r.Equal("first", s.GetString())

	r.NoError(s.SetString("third"))
	r.Equal(third, s.Get(), "what the code that reads it compares")
	r.Equal("third", s.GetString())
	r.Equal([]string{"third"}, applied)

	r.NoError(s.SetString("third"))
	r.Equal([]string{"third"}, applied, "setting the same value again is no change")

	r.NoError(s.Set(second), "and by value, for the code that holds one")
	r.Equal("second", s.GetString())
	r.ErrorIs(s.Set(3), ErrInvalidValue, "no such value")

	r.NoError(s.SetString(""))
	r.Equal("first", s.GetString(), "the empty value is the default")

	r.ErrorIs(s.SetString("fourth"), ErrInvalidValue)
	r.Equal("first", s.GetString(), "a refused value leaves it alone")

	r.Panics(func() { s.NewValue("fourth") }, "a value made after the switch was used")
}

// What a switch is declared with is wrong once or never, so it panics rather
// than reporting.
func Test_EnumDeclaration(t *testing.T) {
	r := require.New(t)

	r.Panics(func() { NewEnum("empty", nil).GetString() }, "an enum without values")
	r.Panics(func() { NewEnum("empty", nil).SetString("x") }, "an enum without values")

	s := NewEnum("dup", nil)
	s.NewValue("one")
	r.Panics(func() { s.NewValue("one") }, "the same value twice")
	r.Panics(func() { s.NewValue("") }, "a value without a name")
}

func Test_Int(t *testing.T) {
	r := require.New(t)
	applied := []int64{}
	s := NewInt("test-int", 10, 1, 100, func(v int64) error { applied = append(applied, v); return nil })

	r.Equal("1..100", s.Syntax())
	r.EqualValues(10, s.Get(), "the default it was made with")
	r.Equal("10", s.GetString())

	r.NoError(s.SetString("42"))
	r.EqualValues(42, s.Get())
	r.Equal([]int64{42}, applied)

	r.NoError(s.SetString("42"))
	r.Equal([]int64{42}, applied, "setting the same value again is no change")

	r.NoError(s.SetString(""))
	r.EqualValues(10, s.Get(), "the empty value is the default")

	r.ErrorIs(s.SetString("101"), ErrInvalidValue, "above the range")
	r.ErrorIs(s.SetString("0"), ErrInvalidValue, "below it")
	r.ErrorIs(s.SetString("ten"), ErrInvalidValue, "no number at all")
	r.EqualValues(10, s.Get(), "a refused value leaves it alone")

	r.NoError(s.Set(1), "a config key that is a number needs no spelling")
	r.EqualValues(1, s.Get())
	r.ErrorIs(s.Set(1000), ErrInvalidValue)

	r.Panics(func() { NewInt("bad", 0, 1, 10, nil) }, "a default out of the range is a mistake to make once")
}
