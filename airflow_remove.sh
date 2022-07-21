#!/bin/bash

helm delete airflow -n airflow
helm delete nfs-subdir-external-provisioner -n storage
AIRFLOW_VOID_STRING=""

# Wait until all airflow pods are down
until [ "${AIRFLOW_VOID_STRING}" = "No resources found in airflow namespace." ]
do
    AIRFLOW_VOID_STRING=$(kubectl get pods -n airflow 2>&1)
    echo $(kubectl get pods -n airflow 2>&1)
done

kubectl delete namespaces airflow storage

echo "Press any key to continue"
while [ true ] ; do
read -t 3 -n 1
if [ $? = 0 ] ; then
exit ;
else
echo "waiting for the keypress"
fi
done