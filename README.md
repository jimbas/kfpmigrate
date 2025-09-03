# kfpmigrate

A simple python codes for migrating kubeflow pipeline cluster to other. It heavily uses kubeflow APIs.

## How To

```
python -u kfpmigrate.py config.json migrate pipelines
```

## Explanations

When migrating pipelines, it checks container images repository. it then automatically replaces with the destination repository then automatically runs docker pull, tag, and push.

## Cautions

There are things that considered dangerous. I intentionally commented the codes to prevent accidentally running the script. Please check the codes first before uncommented them

## References

- [Kubeflow API](https://kubeflow-pipelines.readthedocs.io)

