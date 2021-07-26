import time


"""
Here a message processing function could be defined

For my purposes, this function need to start an appropriate docker image
with correct arguments that come in the message received

The message processor function is to be tested manually. Should it fail when
passed to the App instance, the entire App goes down
"""


def process_message_using_docker_image_sample_1(
    message: str,
    required_docker_image: str = "sample-message-processor:latest",
) -> None:
    """
    Receives a message, parses it, and then launches an appropriate docker
    image with appropriate arguments received in the message
    """
    # TODO: What if container fails or freezes? I need to somehow report it
    # TODO: How to handle docker authentication to pull image from AzureCR?
    # TODO: I need to check if I have the latest version locally
    import docker

    try:
        client = docker.from_env()
    except Exception as e:
        print(f"Failed to init docker client. Error: {e}")
        raise e

    images = client.images.list()
    required_image_present = required_docker_image in [
        image.tags[0] for image in images if image.tags
    ]
    if not required_image_present:
        print("Pulling the required image")
        try:
            client.images.pull(required_docker_image)
        except Exception as e:
            print(f"Failed to pull the requested image. Error: {e}")
            raise e
    else:
        print("Image found locally")

    print("Parsing the message")
    kube_id, _, _ = message.split(";")

    print("Attempting to run the image")
    container = client.containers.run(
        required_docker_image,
        f"--iterate_till {kube_id}",  # Here comes the parsed message
        detach=True,
    )
    print(f"Started the container. Its ID: {container.id}")
    for line in container.logs(stream=True):
        # TODO: Forward logs to Comet
        print(line.strip())

    container.remove()


def process_message(message: str) -> None:
    for i in range(10):
        time.sleep(1)
        print(f"BUSILY PROCESSING THE MESSAGE LMAO! {message} - {0}/{i}")
    return


if __name__ == "__main__":
    process_message_using_docker_image_sample_1("14;d;f")
