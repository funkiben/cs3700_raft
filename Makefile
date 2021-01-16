build:
	cargo build --release -j1
	cp ./target/release/my_project6 ./testbed/3700kvstore
	chmod +x ./testbed/3700kvstore