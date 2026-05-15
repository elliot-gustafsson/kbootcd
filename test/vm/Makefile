.PHONY: all localrun


CONTAINER_IMAGE=localhost/kbootcd-test/rke2:almalinux10

BUILDER_IMAGE=quay.io/centos-bootc/bootc-image-builder:latest


print-container-image:
	@echo $(CONTAINER_IMAGE)

build: build-containerfile build-qcow2

build-containerfile:
	sudo podman build --pull \
		-t $(CONTAINER_IMAGE) \
		-f Containerfile .

build-qcow2:
	mkdir -p build-output
	rm -rf build-output/*
	sudo podman pull $(BUILDER_IMAGE)
	sudo podman run --rm -it --privileged --pull=never \
		--security-opt label=type:unconfined_t \
		-v $(shell pwd)/build-output:/output \
		-v $(shell pwd)/config.toml:/config.toml \
		-v /var/lib/containers/storage:/var/lib/containers/storage \
		$(BUILDER_IMAGE) \
			--type qcow2 \
			--chown "$(shell id -u):$(shell id -g)" \
			--config /config.toml \
			--output /output \
			$(CONTAINER_IMAGE)

# NOTE: CTRL+A then x to quit
localrun: ./build-output/qcow2/disk.qcow2
	podman build -t containerfile-localrun -f Containerfile.runner .
	podman run --rm -v "$(shell pwd)/build-output/qcow2:/data" containerfile-localrun qemu-img create -f qcow2 /data/data-disk.qcow2 20G
	podman run --rm -it --replace \
		--device /dev/kvm \
		--network host \
		--privileged \
		--security-opt label=disable \
		--name os-images-localrun \
		-v "$(shell pwd)/build-output/qcow2/disk.qcow2":/data/disk.qcow2 \
		-v "$(shell pwd)/build-output/qcow2/data-disk.qcow2":/data/data-disk.qcow2 \
		-v ~/.ssh:/.ssh:ro \
		containerfile-localrun \
		/usr/libexec/qemu-kvm \
			-m 4G \
			-smp 2 \
			-cpu host \
			-enable-kvm \
			-snapshot \
			-drive file=/data/disk.qcow2,format=qcow2,if=virtio \
			-drive file=/data/data-disk.qcow2,format=qcow2,if=virtio \
			-drive file=/data/seed.iso,format=raw,if=virtio \
			-device virtio-rng-pci \
			-device virtio-net-pci,netdev=net0,csum=off,gso=off,guest_tso4=off,guest_tso6=off,host_tso4=off,host_tso6=off,mrg_rxbuf=off \
			-netdev user,id=net0,hostfwd=tcp:0.0.0.0:2222-:22,hostfwd=tcp:0.0.0.0:6443-:6443,hostfwd=tcp:0.0.0.0:9345-:9345,hostfwd=tcp:0.0.0.0:8080-:80,hostfwd=tcp:0.0.0.0:8443-:443 \
			-nographic \
			-serial mon:stdio
