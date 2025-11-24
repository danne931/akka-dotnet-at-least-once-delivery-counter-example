namespace ClusterShardingMessageExtractor;

using Akka.Cluster.Sharding;

public interface IWithEntityId
{
    Guid EntityId { get; }
    object Message { get; }
}

public sealed class AppClusteringMessageExtractor(int maxNumberOfShards) : HashCodeMessageExtractor(maxNumberOfShards)
{
    public override string EntityId(object message)
    {
        if (message is IWithEntityId withEntityId)
        {
            return withEntityId.EntityId.ToString();
        }
        return "";
    }

    public override object EntityMessage(object message) => message;
}