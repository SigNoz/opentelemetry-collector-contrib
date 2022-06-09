// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parse(t *testing.T) {
	tests := []struct {
		query    string
		expected *ParsedQuery
	}{
		{
			query: `set("foo")`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							String: strp("foo"),
						},
					},
				},
				Condition: nil,
			},
		},
		{
			query: `met(1.2)`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "met",
					Arguments: []Value{
						{
							Float: floatp(1.2),
						},
					},
				},
				Condition: nil,
			},
		},
		{
			query: `fff(12)`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "fff",
					Arguments: []Value{
						{
							Int: intp(12),
						},
					},
				},
				Condition: nil,
			},
		},
		{
			query: `set("foo", get(bear.honey))`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							String: strp("foo"),
						},
						{
							Invocation: &Invocation{
								Function: "get",
								Arguments: []Value{
									{
										Path: &Path{
											Fields: []Field{
												{
													Name: "bear",
												},
												{
													Name: "honey",
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Condition: nil,
			},
		},
		{
			query: `set(foo.attributes["bar"].cat, "dog")`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							Path: &Path{
								Fields: []Field{
									{
										Name: "foo",
									},
									{
										Name:   "attributes",
										MapKey: strp("bar"),
									},
									{
										Name: "cat",
									},
								},
							},
						},
						{
							String: strp("dog"),
						},
					},
				},
				Condition: nil,
			},
		},
		{
			query: `set(foo.attributes["bar"].cat, "dog") where name == "fido"`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							Path: &Path{
								Fields: []Field{
									{
										Name: "foo",
									},
									{
										Name:   "attributes",
										MapKey: strp("bar"),
									},
									{
										Name: "cat",
									},
								},
							},
						},
						{
							String: strp("dog"),
						},
					},
				},
				Condition: &Condition{
					Left: Value{
						Path: &Path{
							Fields: []Field{
								{
									Name: "name",
								},
							},
						},
					},
					Op: "==",
					Right: Value{
						String: strp("fido"),
					},
				},
			},
		},
		{
			query: `set(foo.attributes["bar"].cat, "dog") where name != "fido"`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							Path: &Path{
								Fields: []Field{
									{
										Name: "foo",
									},
									{
										Name:   "attributes",
										MapKey: strp("bar"),
									},
									{
										Name: "cat",
									},
								},
							},
						},
						{
							String: strp("dog"),
						},
					},
				},
				Condition: &Condition{
					Left: Value{
						Path: &Path{
							Fields: []Field{
								{
									Name: "name",
								},
							},
						},
					},
					Op: "!=",
					Right: Value{
						String: strp("fido"),
					},
				},
			},
		},
		{
			query: `set  ( foo.attributes[ "bar"].cat,   "dog")   where name=="fido"`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							Path: &Path{
								Fields: []Field{
									{
										Name: "foo",
									},
									{
										Name:   "attributes",
										MapKey: strp("bar"),
									},
									{
										Name: "cat",
									},
								},
							},
						},
						{
							String: strp("dog"),
						},
					},
				},
				Condition: &Condition{
					Left: Value{
						Path: &Path{
							Fields: []Field{
								{
									Name: "name",
								},
							},
						},
					},
					Op: "==",
					Right: Value{
						String: strp("fido"),
					},
				},
			},
		},
		{
			query: `set("fo\"o")`,
			expected: &ParsedQuery{
				Invocation: Invocation{
					Function: "set",
					Arguments: []Value{
						{
							String: strp("fo\"o"),
						},
					},
				},
				Condition: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			parsed, err := Parse(tt.query)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, parsed)
		})
	}
}

func Test_parse_failure(t *testing.T) {
	tests := []string{
		`set(`,
		`set("foo)`,
		`set(name.)`,
		`("foo")`,
		`set("foo") where name =||= "fido"`,
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			_, err := Parse(tt)
			assert.Error(t, err)
		})
	}
}

func strp(s string) *string {
	return &s
}

func floatp(f float64) *float64 {
	return &f
}

func intp(i int64) *int64 {
	return &i
}
