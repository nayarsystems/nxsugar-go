package nxsugar

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/xeipuuv/gojsonschema"
)

type Schema struct {
	FromFile string
	Input    string
	Result   string
	Error    string
	Pacts    []SchemaPact
}

type SchemaPact struct {
	Input  string
	Output string
}

type methodSchema struct {
	source    string
	json      interface{}
	validator *gojsonschema.Schema
}

type methodPact struct {
	input  interface{}
	output interface{}
}

/*
AddMethodSchema adds (or replaces if already added) a method for the service with a JSON schema.
The function that receives the nexus.Task should return a result or an error.
If four arguments are provided, the fourth is a function used when calling the method in testing mode.
If the schema validation does not succeed, an ErrInvalidParams error will be sent as a result for the task.
*/
func (s *Service) AddMethodSchema(name string, schema *Schema, f func(*Task) (interface{}, *JsonRpcErr), testf ...func(*Task) (interface{}, *JsonRpcErr)) error {
	if len(testf) != 0 {
		return s.addMethod(name, schema, f, testf[0])
	} else {
		return s.addMethod(name, schema, f, nil)
	}
}

func (s *Service) addSchemaToMethod(name string, schema *Schema) error {
	// Add schema from file
	if schema.FromFile != "" {
		file, err := os.Open(schema.FromFile)
		if err != nil {
			return fmt.Errorf("error adding method (%s) jsonschema from file: %s", name, err.Error())
		}
		var newSch map[string]interface{}
		err = json.NewDecoder(file).Decode(&newSch)
		if err != nil {
			return fmt.Errorf("error adding method (%s) jsonschema from file: %s", name, err.Error())
		}
		if input, ok := newSch["input"]; ok {
			src, _, validator, err := compileSchema("", input)
			if err != nil {
				return fmt.Errorf("error adding method (%s) input jsonschema: %s", name, err.Error())
			}
			s.methods[name].inSchema = &methodSchema{source: src, json: input, validator: validator}
		}
		if result, ok := newSch["result"]; ok {
			src, _, validator, err := compileSchema("", result)
			if err != nil {
				return fmt.Errorf("error adding method (%s) result jsonschema: %s", name, err.Error())
			}
			s.methods[name].resSchema = &methodSchema{source: src, json: result, validator: validator}
		}
		if errs, ok := newSch["error"]; ok {
			src, _, validator, err := compileSchema("", errs)
			if err != nil {
				return fmt.Errorf("error adding method (%s) error jsonschema: %s", name, err.Error())
			}
			s.methods[name].errSchema = &methodSchema{source: src, json: errs, validator: validator}
		}
		if pacts, ok := newSch["pacts"].([]interface{}); ok {
			for n, pact := range pacts {
				if pactm, ok := pact.(map[string]interface{}); ok {
					if pactInput, ok := pactm["input"]; ok {
						if pactOutput, ok := pactm["output"]; ok {
							s.methods[name].pacts = append(s.methods[name].pacts, &methodPact{
								input:  pactInput,
								output: pactOutput,
							})
						} else {
							return fmt.Errorf("error adding method (%s) pact (%d): missing output field", name, n)
						}
					} else {
						return fmt.Errorf("error adding method (%s) pact (%d): missing input field", name, n)
					}
				} else {
					return fmt.Errorf("error adding method (%s) pact (%d): must be map", name, n)
				}
			}
		}
		file.Close()
		return nil
	}

	// Add schema from string
	if schema.Input != "" || schema.Result != "" || schema.Error != "" {
		if schema.Input != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Input)
			if err != nil {
				return fmt.Errorf("error adding method (%s) input jsonschema: %s", name, err.Error())
			}
			s.methods[name].inSchema = &methodSchema{source: schema.Input, json: sch, validator: validator}
		}
		if schema.Result != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Result)
			if err != nil {
				return fmt.Errorf("error adding method (%s) result jsonschema: %s", name, err.Error())
			}
			s.methods[name].resSchema = &methodSchema{source: schema.Result, json: sch, validator: validator}
		}
		if schema.Error != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Error)
			if err != nil {
				return fmt.Errorf("error adding method (%s) error jsonschema: %s", name, err.Error())
			}
			s.methods[name].errSchema = &methodSchema{source: schema.Error, json: sch, validator: validator}
		}
		if schema.Pacts != nil {
			for n, pact := range schema.Pacts {
				methPact := &methodPact{}
				err := json.Unmarshal([]byte(pact.Input), &methPact.input)
				if err != nil {
					return fmt.Errorf("error adding method (%s) pact (%d): parsing input json: %s", name, n, err.Error())
				}
				err = json.Unmarshal([]byte(pact.Output), &methPact.output)
				if err != nil {
					return fmt.Errorf("error adding method (%s) pact (%d): parsing output json: %s", name, n, err.Error())
				}
				s.methods[name].pacts = append(s.methods[name].pacts, methPact)
			}
		}
	}
	return nil
}

func compileSchemaFromJson(source string) (string, interface{}, *gojsonschema.Schema, error) {
	var schres interface{}
	err := json.Unmarshal([]byte(source), &schres)
	if err != nil {
		return "", nil, nil, fmt.Errorf("parsing json: %s", err.Error())
	}
	return compileSchema(source, schres)
}

func compileSchema(source string, d interface{}) (string, interface{}, *gojsonschema.Schema, error) {
	validator, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(d))
	if err != nil {
		return "", nil, nil, fmt.Errorf("invalid: %s", err.Error())
	}
	if source == "" {
		sourceb, err := json.Marshal(d)
		if err != nil {
			return "", nil, nil, fmt.Errorf("invalid: %s", err.Error())
		}
		source = string(sourceb)
	}
	return source, d, validator, nil
}

func schemaValidationErr(result *gojsonschema.Result) string {
	if len(result.Errors()) == 1 {
		return fmt.Sprintf("%s", result.Errors()[0])
	} else {
		out := ""
		for _, desc := range result.Errors() {
			out += fmt.Sprintf("\n- %s", desc)
		}
		return out
	}
}
