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

// Settings of a process that can be changed while it runs, of a few kinds.
//
// A switch is one of the kinds here rather than an implementation of its own:
// what a switch of a kind has to say about itself is the same for all of them,
// which is what lets one api handler serve every one of them. What differs is
// the value it holds, so each kind has an accessor of the type the code that
// reads it wants, and that accessor is a plain atomic load: a switch is read
// often and set once in a while.
//
// The switches of chorus are in pkg/settings. This package knows nothing of
// them and nothing else of chorus either, so that it can be lifted out.

package switches

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// ErrInvalidValue is what setting a switch to something it does not take
// returns, wrapped in what it was.
var ErrInvalidValue = errors.New("invalid switch value")

// What a switch that is either on or off is set to.
const (
	Enabled  = "enabled"
	Disabled = "disabled"
)

// Switch is one setting for the maint api to serve.
type Switch interface {
	// Name is the path segment the api serves the switch under.
	Name() string
	// Syntax says what SetString accepts, for the api to report when it does
	// not.
	Syntax() string
	// GetString and SetString are the switch as the api and a config file
	// speak of it. What the code that reads a switch calls is Get of the kind
	// it is, which answers in the type it wants.
	GetString() string
	// SetString validates the value and does whatever the change entails. The
	// empty value is the default of the switch, which is what a config key
	// that is not set gives.
	SetString(value string) error
}

// Bool is a switch that is either on or off.
type Bool struct {
	name  string
	value atomic.Bool
	def   bool
	apply func(bool) error
}

// NewBool makes a switch of the values enabled and disabled. What the change
// entails besides holding the new value is what apply does, which may be nil.
func NewBool(name string, def bool, apply func(bool) error) *Bool {
	s := &Bool{name: name, def: def, apply: apply}
	s.value.Store(def)
	return s
}

func (s *Bool) Name() string   { return s.name }
func (s *Bool) Syntax() string { return Enabled + ", " + Disabled }

// Get is what the switch holds.
func (s *Bool) Get() bool { return s.value.Load() }

func (s *Bool) GetString() string {
	if s.Get() {
		return Enabled
	}
	return Disabled
}

func (s *Bool) SetString(value string) error {
	switch value {
	case "":
		return s.Set(s.def)
	case Enabled:
		return s.Set(true)
	case Disabled:
		return s.Set(false)
	}
	return fmt.Errorf("%w: %s: unknown value %q, want %s", ErrInvalidValue, s.name, value, s.Syntax())
}

// Set sets the switch from a config key that is a bool.
func (s *Bool) Set(value bool) error {
	if s.value.Swap(value) == value || s.apply == nil {
		return nil
	}
	return s.apply(value)
}

// EnumValue is one value of an enum switch, as the code that reads the switch
// compares it: the position of the value in the switch, not its name.
type EnumValue int32

// Enum is a switch of a fixed set of values, which are made one by one so that
// the name of a value and the value itself cannot come apart:
//
//	var (
//		ListingSpeed = NewEnum("listing-speed", nil)
//		ListingFull        = ListingSpeed.NewValue("full")
//		ListingAuto        = ListingSpeed.NewValue("auto")
//	)
type Enum struct {
	name   string
	value  atomic.Int32
	values []string
	frozen atomic.Bool
	apply  func(string) error
}

// NewEnum makes a switch without values yet, which NewValue gives it. What a
// change entails besides holding the new value is what apply does, which may
// be nil.
func NewEnum(name string, apply func(string) error) *Enum {
	return &Enum{name: name, apply: apply}
}

// NewValue gives the switch another value and returns it. The first one made
// is the default of the switch.
//
// Making a value is for the declaration of a switch and nothing else, so it
// panics once the switch has been used, which no init does and everything else
// does.
func (s *Enum) NewValue(value string) EnumValue {
	if s.frozen.Load() {
		panic("switches: " + s.name + ": a value made after the switch was used")
	}
	if value == "" {
		panic("switches: " + s.name + ": a value without a name")
	}
	for _, v := range s.values {
		if v == value {
			panic("switches: " + s.name + ": the value " + value + " twice")
		}
	}
	s.values = append(s.values, value)
	return EnumValue(len(s.values) - 1)
}

func (s *Enum) Name() string { return s.name }

func (s *Enum) Syntax() string {
	s.use()
	return strings.Join(s.values, ", ")
}

// Get is which of the values the switch holds. Read per request and per
// object, so it is the load and nothing else: that the switch has values at
// all is what its first GetString or SetString finds out, which the api, the
// config and every test do.
func (s *Enum) Get() EnumValue { return EnumValue(s.value.Load()) }

func (s *Enum) GetString() string {
	s.use()
	return s.values[s.Get()]
}

// Set sets the switch to one of its values.
func (s *Enum) Set(value EnumValue) error {
	s.use()
	if value < 0 || int(value) >= len(s.values) {
		return fmt.Errorf("%w: %s: no value %d, want 0..%d", ErrInvalidValue, s.name, value, len(s.values)-1)
	}
	if EnumValue(s.value.Swap(int32(value))) == value || s.apply == nil {
		return nil
	}
	return s.apply(s.values[value])
}

// use marks the switch as in use, which is what makes a value made later a
// mistake to make once rather than a race to find later.
func (s *Enum) use() {
	if s.frozen.Swap(true) || len(s.values) != 0 {
		return
	}
	panic("switches: " + s.name + ": an enum without values")
}

func (s *Enum) SetString(value string) error {
	s.use()
	if value == "" {
		return s.Set(0)
	}
	for i, v := range s.values {
		if v == value {
			return s.Set(EnumValue(i))
		}
	}
	return fmt.Errorf("%w: %s: unknown value %q, want %s", ErrInvalidValue, s.name, value, s.Syntax())
}

// Int is a switch holding a number of a range.
type Int struct {
	name     string
	value    atomic.Int64
	def      int64
	min, max int64
	apply    func(int64) error
}

// NewInt makes a switch of the numbers from min to max, both included. What
// the change entails besides holding the new value is what apply does, which
// may be nil.
func NewInt(name string, def, min, max int64, apply func(int64) error) *Int {
	if def < min || def > max {
		panic("switches: an int whose default is out of its range")
	}
	s := &Int{name: name, def: def, min: min, max: max, apply: apply}
	s.value.Store(def)
	return s
}

func (s *Int) Name() string   { return s.name }
func (s *Int) Syntax() string { return fmt.Sprintf("%d..%d", s.min, s.max) }

// Get is what the switch holds.
func (s *Int) Get() int64 { return s.value.Load() }

func (s *Int) GetString() string { return strconv.FormatInt(s.Get(), 10) }

func (s *Int) SetString(value string) error {
	if value == "" {
		return s.Set(s.def)
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: %s: %q is no number, want %s", ErrInvalidValue, s.name, value, s.Syntax())
	}
	return s.Set(n)
}

// Set sets the switch from a config key that is a number.
func (s *Int) Set(value int64) error {
	if value < s.min || value > s.max {
		return fmt.Errorf("%w: %s: %d is out of %s", ErrInvalidValue, s.name, value, s.Syntax())
	}
	if s.value.Swap(value) == value || s.apply == nil {
		return nil
	}
	return s.apply(value)
}
