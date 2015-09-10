package de.codepitbull.vertx.eventsourcing.constants;

/**
 * Enum for the failure-codes used by this project for signalling a problem as part of an event-reply.
 */
public enum FailureCodesEnum {
    FAILURE_UNABLE_TO_DEPLOY_GAME_VERTICLE(0),
    FAILURE_UNABLE_TO_DELETE_NON_EXISTING_GAME(1),
    FAILURE_UNABLE_TO_UNDEPLOY_GAME_VERTICLE(2),
    FAILURE_GAME_DOES_NOT_EXIST(3),
    FAILURE_GAME_FULL(4),
    FAILURE_MISSING_PARAMETER(5);

    private final int intValue;

    private FailureCodesEnum(int intValue) {
        this.intValue = intValue;
    }

    public int intValue() {
        return intValue;
    }
}