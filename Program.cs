using Akka.Actor;
using Akka.Hosting;
using Akka.Cluster.Hosting;
using Akka.Persistence.Hosting;
using Akka.Remote.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

using Counter;
using GuaranteedDelivery;
using ClusterShardingMessageExtractor;

var hostname = "localhost";
var port = 5000;
var shardRegionRole = "demo-counters";

var host = new HostBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddAkka("CounterSystem", (builder, sp) =>
        {
            builder
                .AddHocon(
                    "akka.reliable-delivery.sharding.consumer-controller.allow-bypass: true",
                    HoconAddMode.Prepend
                )
                .WithInMemoryJournal()
                .WithInMemorySnapshotStore()
                .WithRemoting(hostname, port)
                .WithClustering(new ClusterOptions
                {
                    Roles = [ shardRegionRole ],
                    SeedNodes = [ $"akka.tcp://CounterSystem@{hostname}:{port}" ]
                })
                .WithShardRegion<CounterActor>(
                    typeName: "counter",
                    entityPropsFactory: entityId =>
                        ClusterShardingConsumer.Create<Message.IncrementCounter>(
                            sp.GetRequiredService<ActorSystem>(),
                            ctrl => CounterActor.Props(entityId, ctrl)
                        ),
                    messageExtractor: new AppClusteringMessageExtractor(100),
                    shardOptions: new ShardOptions
                    {
                        Role = shardRegionRole,
                        RememberEntities = false
                    }
                )
                .WithActors((system, registry) => {
                    system.ActorOf(
                        Props.Create(() => new Util.DeadletterMonitor(system)),
                        "DeadLetterMonitoringActor"
                    );

                    var shardRegion = registry.Get<CounterActor>();
                    var producerOpts = new ClusterShardingProducerOptions(
                        system,
                        shardRegion,
                        "counter-producer"
                    );
                    registry.Register<ICounterActorProducerMarker>(
                        ClusterShardingProducer.Create<Message.IncrementCounter>(producerOpts)
                    );
                })
                .AddStartup((system, registry) =>
                {
                    var producer = registry.Get<ICounterActorProducerMarker>();

                    var counterId = Guid.NewGuid();

                    for (int i = 0; i < 9310; i++)
                    {
                        producer.Tell(
                            new AtLeastOnceDeliveryMessage<Message.IncrementCounter>(
                                counterId,
                                new Message.IncrementCounter(counterId)
                            )
                        );
                    }

                    // Log the counter state
                    system.Scheduler.ScheduleTellRepeatedly(
                        TimeSpan.FromSeconds(0),
                        TimeSpan.FromSeconds(.5),
                        registry.Get<CounterActor>(),
                        new Message.GetCounter(counterId),
                        system.ActorOf(Props.Create(() => new CounterResponseHandler()))
                    );
                });
        });
    })
    .Build();

await host.StartAsync();

Console.WriteLine("Akka.NET AtLeastOnceDelivery cluster sharding example is running...");
Console.WriteLine("Press Ctrl+C to stop...");

// Keep the application running
var waitUntilUserExits = new TaskCompletionSource<bool>();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    waitUntilUserExits.SetResult(true);
};

await waitUntilUserExits.Task;

Console.WriteLine("Shutting down...");

await host.StopAsync();

namespace Util
{
    using Akka.Event;

    public class DeadletterMonitor : ReceiveActor
    {
        public DeadletterMonitor(ActorSystem system)
        {
            system.EventStream.Subscribe(Self, typeof(DeadLetter));

            Receive<DeadLetter>(dl =>
                Console.WriteLine($"DeadLetter captured: {dl.Message}, sender: {dl.Sender}, recipient: {dl.Recipient}")
            );
        }
    }
}