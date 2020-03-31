# plainkv-fs

An FUSE wrapper around [PlainKV](https://github.com/roy2220/plainkv)

## Requirements

```bash
# Fedora
sudo yum install -y fuse

# Ubuntu
sudo apt-get install -y fuse

```

## Mounting

```bash
go get github.com/roy2220/plainkv-fs/cmd/plainkv-fs
go build -o plainkv-fs github.com/roy2220/plainkv-fs/cmd/plainkv-fs

mkdir -p ./my_mnt
./plainkv-fs ./test.db ./my_mnt
# get blocked, send CTRL-C to stop.
```

## Testing

```bash
echo 'hello world!' > ./my_mnt/a.txt
cat ./my_mnt/a.txt
```
