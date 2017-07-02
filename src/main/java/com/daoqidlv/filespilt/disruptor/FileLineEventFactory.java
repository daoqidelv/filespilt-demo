package com.daoqidlv.filespilt.disruptor;

import com.lmax.disruptor.EventFactory;

public class FileLineEventFactory implements EventFactory<FileLine>
{
    public FileLine newInstance()
    {
        return new FileLine();
    }
}