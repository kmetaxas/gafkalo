package state

import (
	"os"

	internalconfig "github.com/kmetaxas/gafkalo/internal/config"
	"github.com/kmetaxas/srclient"
	"github.com/nsf/jsondiff"
	log "github.com/sirupsen/logrus"
)

type Schema struct {
	SubjectName   string
	SchemaPath    string `yaml:"schema"`
	Compatibility string `yaml:"compatibility"`
	SchemaData    string
	SchemaType    srclient.SchemaType `yaml:"schema_type"`
}

func (s *Schema) SchemaDiff(other string) (bool, string) {
	var isEqual bool
	var diffString string
	var cmpRes jsondiff.Difference
	opts := jsondiff.DefaultConsoleOptions()
	cmpRes, diffString = jsondiff.Compare([]byte(s.SchemaData), []byte(other), &opts)
	isEqual = cmpRes == jsondiff.FullMatch

	return isEqual, diffString
}

func CreateSchema(SubjectName string, SchemaPath string, Compatibility string, SchemaType srclient.SchemaType) (Schema, error) {
	var newSchema Schema
	newSchema.SubjectName = SubjectName
	newSchema.SchemaPath = SchemaPath
	newSchema.Compatibility = Compatibility

	pathToSchemaFile := internalconfig.NormalizeSchemaPath(SchemaPath)
	if pathToSchemaFile != "" && SchemaPath != "" {
		data, err := os.ReadFile(pathToSchemaFile)
		if err != nil {
			log.Fatalf("Unable to create schema with Error: %s\n", err)
		}
		newSchema.SchemaData = string(data)
		if SchemaType == "" {
			newSchema.SchemaType = "AVRO"
		} else {
			newSchema.SchemaType = SchemaType
		}
	}
	return newSchema, nil
}

func (s *Schema) UnmarshalYAML(unmarshal func(any) error) error {
	type rawSchema Schema
	raw := rawSchema{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	var err error
	*s, err = CreateSchema(raw.SubjectName, raw.SchemaPath, raw.Compatibility, raw.SchemaType)
	if err != nil {
		return err
	}
	return nil
}
