# Operato Foundatiton Service

The Integration Platform is the platform that will be responsible for the integration layer in your enterprise architecture.

The modules that make up this platform are
- Integration modeler: The modeler is a board-based implementation of the Enterprise Integration Patterns (EIP) concept.
- Integration Engine: This will be based on the existing scenario engine.
- Integration connectors: Various connectors will be developed as pipeline modules. The interworking structure of the pipeline modules and the engine is being prepared through the EGI project.
- Integration management service: It will be developed based on our framework.

In addition to the above components, there are microservices that are essential for all platform configurations, which we call foundation services, and currently consist of the following.

- [Scheduler service](./scheduler-service/README.md): scheduling events (PUBSUB mode)
- [Delay Scheduler Service](./pending-queue-service/README.md): scheduling events (POLLING mode)
- [Message Hub Service](./service-broker/README.md): Dynamic Service Endpoint Management Service

