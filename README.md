# rl-scheduler

# Генерация го файлов из .proto

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

# Запуск

1. Нужно запустить MinIO хранилище с помощью docker compose (директория storage). Так же нужно будет создать там бакет и указать его во всех конфигах.

