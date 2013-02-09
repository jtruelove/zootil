package com.zootil.election;

public interface IElectable
{
    void leadershipChanged(boolean amILeader);
    void terminatingEventOcurred(Exception e);
}
