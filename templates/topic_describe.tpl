---
Topic: {{ .Name }}
ReplicationFactor {{ .Details.ReplicationFactor }} , Partitions: {{ .Details.NumPartitions }}
Configs: {{ range $confName, $confVal := .Details.ConfigEntries -}}{{- $confName -}}={{- $confVal -}}, {{ end }}
{{ range $partitionNum , $replicaIds := .Details.ReplicaAssignment -}}
Partition {{ $partitionNum }}: Leader ID {{ index $replicaIds 0 }}. Followers: {{slice  $replicaIds 1  }}
{{ end -}}
