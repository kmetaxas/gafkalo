------
## Topics
{{ range .Topics -}}
{{ if .HasErrors }}[ERROR]{{ if $.IsPlan }}[PLAN]{{end}}  Create/Update Topic {{ .Name }} {{if $.IsPlan}} would have {{end}}failed with errors: 
{{- range .Errors }}
  - {{ . }} 
{{ end -}} 
{{ else -}}
{{ if $.IsPlan }}[Plan] Will {{if .IsNew}}Create{{else}}Update{{end}} {{ else }} {{if .IsNew}}Created{{else}}Update{{end}} {{ end }} Topic {{ .Name }} Partitions: {{ .NewPartitions}} ReplicationFactor: {{ .NewReplicationFactor }} {{ if .HasChangedConfigs}}Non-default configs:{{else}}(default configs){{end}}
  {{- range .ChangedConfigs }} 
  - Config {{ .Name }} changed from {{ .OldVal }} to {{ .NewVal }}
  {{- end }} 
{{ if .PartitionsChanged -}} 
Partitions {{ if $.IsPlan }}will be {{end}}changed to {{.NewPartitions}} from {{.OldPartitions}} New partitions:
  | Partition | Brokers |
{{- range $partition, $brokers:=  .ReplicaPlan }}
  | {{ $partition }} | {{range $broker := $brokers}}{{$broker}},{{end}} |
{{- end}}
{{- end }}
{{- end }}
{{- end }}
## Schemas
{{ range .Schemas -}}
{{ if or .Changed .HasNewCompatibility -}} 
{{ if $.IsPlan -}}
[PLAN] -  Subject {{ .SubjectName }} {{ if .Changed }}will be registered with a new version.{{- end }} {{ if .HasNewCompatibility }} Compatibility will be set to {{ .NewCompat }}{{- end }}
{{ else -}}
Subject {{ .SubjectName }}: 
{{- if .HasNewVersion -}} Registered with new version {{ .NewVersion }}. {{- end -}}
{{if .HasNewCompatibility }} Changed Compatibility to {{ .NewCompat }}.{{end}}
{{ end -}}
{{- end -}}
{{- end }}{{/* .Changed .HasNewCompatibility  */}}
## Roles and Clients
{{ range .Clients -}}
{{ if $.IsPlan }}[PLAN]. Will add{{ else }} Added {{ end }} role {{ .Role }} to principal {{ .Principal }} for {{ .ResourceType }}:{{ .ResourceName }} with type {{ .PatternType }}
{{ end }}
## Connectors
{{ range $connector := .Connectors }}
{{- if $.IsPlan }}[PLAN] Will create/update{{ else }}Created/updated{{ end }} connector {{ $connector.Name }}. Configs:
{{ range $confKey, $confVal := $connector.NewConfigs }}
{{- $oldValue := index $connector.OldConfigs $confKey -}}
{{"\t"}}{{ $confKey }}: {{ HideSensitive $confKey $confVal (index $.ExtraContextKeys "sensitive_regex")  }}{{ if ne $oldValue $confVal }} (Old value: {{ $oldValue }}){{ end }}
{{- if index $connector.Errors $confKey -}}
{{ range (index $connector.Errors $confKey) }} {{"\n\t\t"}}- Validation ERROR: "{{.}}" {{ end }}
{{- end -}} {{/* If errors */}}
{{ end }} {{/*End range over NewConfigs */}}
{{- end }}
