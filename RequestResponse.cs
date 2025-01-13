namespace RabbitHelper
{
    public enum StatusOptions
    {
        SUCCESS,
        FAIL,
        ERROR
    }

    public struct Response<T> where T : class
    {
        StatusOptions Status;
        string Error;
        T Item;
    }
}
