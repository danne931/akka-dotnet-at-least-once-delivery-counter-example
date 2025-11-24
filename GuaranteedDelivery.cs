namespace GuaranteedDelivery;

using Akka.Actor;
using Akka.Cluster.Sharding;
using Akka.Cluster.Sharding.Delivery;
using Akka.Delivery;

public record AtLeastOnceDeliveryMessage<Msg>(Guid EntityId, Msg Message);

public class ClusterShardingProducerActor<Msg> : ReceiveActor, IWithUnboundedStash
{
    public IStash Stash { get; set; } = null!;
    public IActorRef SendNext { get; set; } = ActorRefs.Nobody;

    public ClusterShardingProducerActor()
    {
        Idle();
    }

    private void Idle()
    {
        Receive<AtLeastOnceDeliveryMessage<Msg>>(_ =>
        {
            Stash.Stash();
        });

        Receive<ShardingProducerController.RequestNext<Msg>>(next =>
        {
            SendNext = next.SendNextTo;

            Become(Active);

            Stash.UnstashAll();
        });
    }

    private void Active()
    {
        Receive<AtLeastOnceDeliveryMessage<Msg>>(cmd =>
        {
            SendNext.Tell(new ShardingEnvelope(cmd.EntityId.ToString(), cmd.Message!));

            Become(Idle); // Wait for demand
        });

        Receive<ShardingProducerController.RequestNext<Msg>>(next =>
        {
            SendNext = next.SendNextTo;
        });
    }
}

public record ClusterShardingProducerOptions(
    ActorSystem System,
    IActorRef ShardRegion,
    string ProducerName
);

public static class ClusterShardingProducer
{
    public static IActorRef Create<Msg>(ClusterShardingProducerOptions opts)
    {
        var system = opts.System;
        var clusterMemberAddress = Akka.Cluster.Cluster.Get(system).SelfAddress;
        var hash = Akka.Util.MurmurHash.StringHash(clusterMemberAddress.ToString());
        var producerId = opts.ProducerName + hash;

        var shardingProducerControllerProps =
            ShardingProducerController.Create<Msg>(
                producerId,
                opts.ShardRegion,
                Akka.Util.Option<Props>.None,
                ShardingProducerController.Settings.Create(system)
            );

        var producerControllerRef =
            system.ActorOf(
                shardingProducerControllerProps,
                $"sharding-producer-controller-{opts.ProducerName}"
            );

        var producer =
            system.ActorOf(
                Props.Create<ClusterShardingProducerActor<Msg>>(),
                opts.ProducerName
            );

        var startMsg = new ShardingProducerController.Start<Msg>(producer);
        producerControllerRef.Tell(startMsg);

        return producer;
    }
}

/// ShardingConsumerController guarantees processing of messages,
/// even across process restarts, shutdowns or shard rebalancing.
public static class ClusterShardingConsumer
{
    public static Props Create<Msg>(ActorSystem system, Func<IActorRef, Props> factory)
    {
        return ShardingConsumerController.Create<Msg>(
            factory,
            ShardingConsumerController.Settings.Create(system)
        );
    }
}

public static class ConsumerControllerDeliveryExtensions
{
    /// Notify Akka Guaranteed Delivery Controller of successful
    /// delivery to a cluster sharded entity actor.
    public static void Ack<Msg>(this ConsumerController.Delivery<Msg> message)
    {
        message.ConfirmTo.Tell(ConsumerController.Confirmed.Instance);
    }
}