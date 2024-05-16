cd internal/manifest
go test -v -timeout 2000s
cd -

cd bithash
go test -v -timeout 2000s
cd -

cd bitpage
go test -v -timeout 2000s
cd -

cd bitree
go test -v -timeout 2000s
cd -

cd bitforest
go test -v -timeout 2000s
cd -

go test -v -timeout 3600s