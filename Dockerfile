FROM quay.io/astronomer/astro-runtime:6.0.3

ENV AIRFLOW_VAR_MY_DAG_PARTNER='{"name":"partner_a","api_secret":"my_secret","path":"/tmp/partner_a"}'