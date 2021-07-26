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
    import docker

    try:
        client = docker.from_env()
    except Exception as e:
        print(f"Failed to init docker client. Error: {e}")
        raise e

    # TODO: For now it is assumed the image required is present locally and
    #       is of correct version (latest) - authentication/AzureSDK issues
    images = client.images.list()
    required_image_present = required_docker_image in [
        image.tags[0] for image in images if image.tags
    ]
    if not required_image_present:
        raise NotImplementedError("Docker image pulling not implemented")
        # print("Pulling the required image")
        # try:
        #     client.images.pull(required_docker_image)
        # except Exception as e:
        #     print(f"Failed to pull the requested image. Error: {e}")
        #     raise e
    else:
        print("Image found locally")

    kube_id, _, _ = message.split(";")

    """
    A container could be run in detached mode, then .run() return Container, 
    else the calling is blocking and returns logs
    """
    container_logs = client.containers.run(
        image=required_docker_image,
        command=f"--iterate_till {kube_id}",  # Here comes the parsed message
        remove=True,
    )
    print("\n\nLOGS:", container_logs.decode("utf-8"))
    # TODO: Upload logs to Comet

    # TODO: What if container fails or freezes? I need to somehow report it -
    #       throw an exception here that must be handles by the runner
    #       Check container status? Check the logs?
    client.close()


def dummy_process_message(message: str) -> None:
    for i in range(10):
        time.sleep(1)
        print(f"BUSILY PROCESSING THE MESSAGE! {message} - {0}/{i}")
    return


if __name__ == "__main__":
    process_message_using_docker_image_sample_1("14;d;f")
