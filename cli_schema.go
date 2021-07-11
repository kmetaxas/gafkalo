package main

import (
	"fmt"
	"log"
)

type SchemaCmd struct {
	CheckExists CheckExistsCmd `cmd help:"Check if provided schema is registered"`
	SchemaDiff  SchemaDiffCmd  `cmd help:"Get the diff between a schema file and a registered schema"`
}

type CheckExistsCmd struct {
	SchemaFile string `required help:"Schema file to checj"`
	Subject    string `required help:"Subject to check against schema"`
}
type SchemaDiffCmd struct {
	SchemaFile string `required help:"Schema file to checj"`
	Subject    string `required help:"Subject to check against schema"`
	Version    int    `required help:"Version to check against"`
}

// Check if a schema is registered in specified subject. Shows version and Id if it is
func (cmd *CheckExistsCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	_, sradmin, _ := GetAdminClients(config)
	schema, err := CreateSchema(cmd.Subject, cmd.SchemaFile, "BACKWARD", "AVRO")
	if err != nil {
		log.Fatal(err)
	}
	schemaID, schemaVersion, err := sradmin.LookupSchema(schema)
	if err != nil {
		return fmt.Errorf("failed to lookup schema [%+vs]", schema)
	}
	if schemaID == 0 {
		fmt.Printf("Schema not found in subject %s\n", schema.SubjectName)
		fmt.Printf("Did not find schema: %+vs\n", schema)
	} else {

		fmt.Printf("Schema is registered under %s with version %d and ID %d\n", schema.SubjectName, schemaVersion, schemaID)
	}
	return nil

}

// compared specified schema file against specified subject/version and produce a diff
// usefull to identify schemas that differ only in newlines or comments and schemaregistry considers it a new schema
func (cmd *SchemaDiffCmd) Run(ctx *CLIContext) error {
	config := LoadConfig(ctx.Config)
	_, sradmin, _ := GetAdminClients(config)
	schema, err := CreateSchema(cmd.Subject, cmd.SchemaFile, "BACKWARD", "AVRO")
	if err != nil {
		return err
	}
	existingSchema, err := sradmin.Client.GetSchemaByVersionWithArbitrarySubject(cmd.Subject, cmd.Version)
	if err != nil {
		return err
	}
	isEqual, diffString := schema.SchemaDiff(existingSchema.Schema())
	if isEqual {
		fmt.Printf("Schemas are equal\n")
	} else {
		fmt.Printf("Schemas are NOT equal. Below is a diff:\n")
		fmt.Printf("%s\n", diffString)
	}
	return nil

}
