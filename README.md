# rl-scheduler

## Генерация го файлов из .proto

Предварительно надо установить утилиту `buf` и `protoc` для golang, на MacOS например вот так:

```
brew install bufbuild/buf/buf
brew install protoc-gen-go
brew install protoc-gen-go-grpc
```

А потом запустить в директории `cluster/proto`:

```
buf generate
```

## Снятие замеров на кластере 
```
cd rl-scheduler/cluster
go run ./cmd/profile --config ../local/profile.yaml        # Для единичных задач
go run ./cmd/profile --config ../local/profile_pairs.yaml  # Для задач по две
```

## Визуализация результатов

Для визуализации результатов есть Jupyter Notebook `rl-scheduler/graph/analysis.ipynb`

## Обучение модели на симуляции кластера (после снятия замеров на кластере)

```
cd rl-scheduler/rl
python train.py
```


## Запуск сервиса инференса (после того как модель обучена)

```
cd rl-scheduler/inference                                 
  MODEL_PATH="../rl/models/best/best_model.zip" python -m uvicorn main:app --port 8000
```
