go test -v -timeout 3600s

cd bithash
go test -v -timeout 2000s
cd -

cd bitpage
go test -v -timeout 2000s
cd -

cd bitree
go test -v -timeout 2000s
cd -

cd bitpage/vectorindex
go test -v -timeout 2000s
cd -

cd bitpage/vectorindex64
go test -v -timeout 2000s
cd -

cd internal/vectormap
go test -v -timeout 2000s
cd -

cd internal/vectortable
go test -v -timeout 2000s
cd -