#!/bin/sh
export KUBECONFIG=/etc/rancher/rke2/rke2.yaml
export CONTAINER_RUNTIME_ENDPOINT=unix:///run/k3s/containerd/containerd.sock
export DATASTORE_TYPE=kubernetes
export PATH="/usr/local/bin:/var/lib/rancher/rke2/bin:$PATH"
