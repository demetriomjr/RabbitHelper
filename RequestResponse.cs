namespace RabbitHelper
{
    public enum StatusOptions
    {
        SUCCESS,
        FAIL,
        ERROR
    }
    public struct Request
    {
        Guid TrackId;
    }

    public struct Response<T> where T : class
    {
        StatusOptions Status;
        string Error;
        T Item;
    }
}
