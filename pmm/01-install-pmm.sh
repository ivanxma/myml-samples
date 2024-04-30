helm repo add percona https://percona.github.io/percona-helm-charts/
helm install pmm -n pmm \
--set secret.create=false \
--set secret.name=pmm-secret \
percona/pmm

