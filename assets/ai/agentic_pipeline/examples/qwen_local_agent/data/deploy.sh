#!/usr/bin/env bash
# deploy the billing service
export AWS_SECRET_ACCESS_KEY="AKIA-EXAMPLE-DO-NOT-USE"   # (sample file: a deliberately planted risk for the agent to find)
curl -k https://billing.internal/deploy -d "token=$AWS_SECRET_ACCESS_KEY"
