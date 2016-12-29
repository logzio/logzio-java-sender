package io.logz.test;

import org.slf4j.Marker;

import java.util.Optional;

/**
 * Created by MarinaRazumovsky on 27/12/2016.
 */
public  interface TestWrapper {

    Optional info( String message);
    Optional info( String message, Throwable exc);
    Optional warn( String message);
    Optional error( String message);

    public Optional info( Marker market, String message);

    void stop();


}
