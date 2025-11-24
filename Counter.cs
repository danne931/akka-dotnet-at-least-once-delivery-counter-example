namespace Counter;

using Akka.Actor;
using Akka.Persistence;
using Akka.Delivery;

using static GuaranteedDelivery.ConsumerControllerDeliveryExtensions;
using ClusterShardingMessageExtractor;

public static class Message
{
    public sealed class IncrementCounter(Guid entityId) : IWithEntityId
    {
        public Guid EntityId { get; } = entityId;
        public object Message => this;
    }

    public sealed class GetCounter(Guid entityId) : IWithEntityId
    {
        public Guid EntityId { get; } = entityId;
        public object Message => this;
    }
}

public static class Event
{
    public sealed class CounterIncremented {}
}

public sealed record CounterState(int Value)
{
    public CounterState Increment() => new(Value + 1);
}

public class CounterActor : ReceivePersistentActor
{
    public override string PersistenceId { get; }

    private CounterState _state = new(0);
    private readonly IActorRef _consumerController;

    public CounterActor(string persistenceId, IActorRef consumerController)
    {
        PersistenceId = persistenceId;
        _consumerController = consumerController;

        Recover<Event.CounterIncremented>(_ => _state = _state.Increment());

        Command<Message.IncrementCounter>(cmd =>
        {
            Persist(new Event.CounterIncremented(), _ =>
            {
                _state = _state.Increment();
            });
        });

        Command<Message.GetCounter>(_ =>
        {
            Sender.Tell(_state.Value);
        });

        Command<ConsumerController.Delivery<Message.IncrementCounter>>(msg =>
        {
            msg.Ack();

            Self.Tell(msg.Message);
        });
    }

    protected override void PreStart()
    {
        _consumerController.Tell(new ConsumerController.Start<Message.IncrementCounter>(Self));
    }

    public static Props Props(string persistenceId, IActorRef consumerController) =>
        Akka.Actor.Props.Create(() => new CounterActor(persistenceId, consumerController));
}

public interface ICounterActorProducerMarker {}

// Actor logs counter state
public class CounterResponseHandler : ReceiveActor
{
    public CounterResponseHandler()
    {
        Receive<int>(count =>
        {
            Console.WriteLine($"Current counter value: {count}");
        });
    }
}