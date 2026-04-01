using Microsoft.Extensions.Logging;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

public sealed class CallHandler
{
    private readonly BotSettings _settings;
    private readonly ILogger<CallHandler> _logger;
    private ICommunicationsClient? _communicationsClient;

    public CallHandler(BotSettings settings, ILogger<CallHandler> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public void Initialize(ICommunicationsClient communicationsClient)
    {
        _communicationsClient = communicationsClient;
        _communicationsClient.Calls().OnIncoming += OnIncomingCall;
    }

    public async Task<ICall> JoinMeetingByUrlAsync(string joinUrl, MediaHandler mediaHandler)
    {
        if (_communicationsClient is null)
        {
            throw new InvalidOperationException("Communications client has not been initialized.");
        }

        IMediaSession mediaSession = mediaHandler.CreateMediaSession(_communicationsClient);
        var (chatInfo, meetingInfo) = CreateJoinInfoFromUrl(joinUrl);

        var joinMeetingParameters = new JoinMeetingParameters(
            chatInfo,
            meetingInfo,
            mediaSession,
            null,
            null,
            false)
        {
            TenantId = _settings.TenantId
        };

        ICall call = await _communicationsClient
            .Calls()
            .AddAsync(joinMeetingParameters);

        call.OnUpdated += (_, args) =>
        {
            _logger.LogInformation("Call state updated. State: {State}", args.NewResource?.State);
        };

        _logger.LogInformation("Join request submitted. Call ID: {CallId}", call.Id);
        return call;
    }

    private void OnIncomingCall(ICallCollection _, CollectionEventArgs<ICall> args)
    {
        foreach (ICall incomingCall in args.AddedResources)
        {
            _logger.LogInformation("Incoming call received. Call ID: {CallId}", incomingCall.Id);
        }
    }

    /// <summary>
    /// Parses a Teams meeting join URL (meetup-join) into ChatInfo / MeetingInfo.
    /// ThreadId must be the thread segment (e.g. 19:meeting_...@thread.v2), not the full URL — otherwise Graph returns 404 NotFound.
    /// </summary>
    private static (ChatInfo ChatInfo, MeetingInfo MeetingInfo) CreateJoinInfoFromUrl(string joinUrl)
    {
        if (string.IsNullOrWhiteSpace(joinUrl))
        {
            throw new ArgumentException("Join URL is empty.", nameof(joinUrl));
        }

        var uri = new Uri(joinUrl.Trim());
        var segments = uri.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

        var meetupIdx = Array.FindIndex(
            segments,
            s => s.Equals("meetup-join", StringComparison.OrdinalIgnoreCase));

        if (meetupIdx < 0 || meetupIdx + 2 >= segments.Length)
        {
            throw new ArgumentException(
                "Invalid Teams join URL. Use a full meeting link like https://teams.microsoft.com/l/meetup-join/19%3A.../0?...",
                nameof(joinUrl));
        }

        var threadId = Uri.UnescapeDataString(segments[meetupIdx + 1]);
        var messageId = Uri.UnescapeDataString(segments[meetupIdx + 2]);

        var chatInfo = new ChatInfo
        {
            ThreadId = threadId,
            MessageId = messageId
        };

        var meetingInfo = new OrganizerMeetingInfo
        {
            Organizer = new IdentitySet
            {
                User = new Identity
                {
                    Id = "00000000-0000-0000-0000-000000000000"
                }
            }
        };

        meetingInfo.AdditionalData = new Dictionary<string, object>
        {
            ["joinWebUrl"] = joinUrl
        };

        return (chatInfo, meetingInfo);
    }
}
