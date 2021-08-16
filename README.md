## kubernetes permissions ##

For each namespace:

1) create a clusterrole (clusterwide) that gives:
   ```
   roleRef:
     apiGroup: rbac.authorization.k8s.io
     kind: ClusterRole
     name: system:controller:pv-protection-controller
  ```
  to:
  ```
   subjects:
   - kind: ServiceAccount
     name: podreader
     namespace: eos-dev1
  ```

2) create a role (in namespace) that gives GET to pods,pvc,pv:
```
   - apiGroups:
     - '*'
     resources:
     - pods
     - persistentvolumeclaims
     - persistentvolumes
     verbs:
     - get
```

3) create a serviceaccount (in that namespace) with a clear name

4) create a rolebinding (in that namespace) for that "Role" to that "ServiceAccount"
   ```
   roleRef:
     apiGroup: rbac.authorization.k8s.io
     kind: Role
     name: podread
   subjects:
   - kind: ServiceAccount
     name: podreader
   ```

## GCP permissions ##

* You will need a custom role for creating snapshots, and associate that role to the serviceaccount used by this pod (through ENV vars and stuff...)
