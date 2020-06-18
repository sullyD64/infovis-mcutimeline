#!/bin/bash
docker-compose -f db_scripts/docker-compose.yml stop 
docker rm django_postgres