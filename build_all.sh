#!/bin/bash

for dir in */ ; do
  if [[ -d "$dir" ]]; then
    docker build -t gcr.io/$PROJECT_ID/${dir%/} $dir
    docker push gcr.io/$PROJECT_ID/${dir%/}
  fi
done
