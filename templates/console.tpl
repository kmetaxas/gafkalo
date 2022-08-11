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
-{{ if $.IsPlan -}}[PLAN] -  Subject {{ .SubjectName }} {{ if .Changed }}will be registered with a new version.{{- end }} {{ if .HasNewCompatibility }} Compatibility will be set to {{ .NewCompat }}{{- end }}
{{ else }}Subject {{ .SubjectName }}: {{ if .Changed }}Registered with new version {{ if eq .NewVersion 0 }}(Known after apply){{ else }}{{ .NewVersion }}.{{- end }}{{if .HasNewCompatibility }} Changed Compatibility to {{ .NewCompat }}.{{end}}
{{ end}}
{{- end }}
{{- end }}
{{- end }}
## Roles and Clients
{{ range .Clients -}}
{{ if $.IsPlan }}[PLAN]. Will add{{ else }} Added {{ end }} role {{ .Role }} to principal {{ .Principal }} for {{ .ResourceType }}:{{ .ResourceName }} with type {{ .PatternType }}
{{ end }}

