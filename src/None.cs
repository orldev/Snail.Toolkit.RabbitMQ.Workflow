namespace Snail.Toolkit.RabbitMQ.Workflow;

/// <summary>
/// The payload of a step that consumes nothing.
/// </summary>
/// <remarks>
/// The first step of a plan has no predecessor to take data from, and a step may legitimately
/// produce no data for the next one. Modelling that as <c>null</c> would make "no payload" and
/// "payload that failed to arrive" the same value, so the absence is given a type of its own.
/// </remarks>
public readonly record struct None;
