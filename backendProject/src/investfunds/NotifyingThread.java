/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 * @author rlcancian
 */
public abstract class NotifyingThread extends Thread {

    private final Set<ThreadCompleteListener> listeners = new CopyOnWriteArraySet<>();

	/**
	 *
	 * @param listener
	 */
	public final void addListener(final ThreadCompleteListener listener) {
        listeners.add(listener);
    }

	/**
	 *
	 * @param listener
	 */
	public final void removeListener(final ThreadCompleteListener listener) {
        listeners.remove(listener);
    }

    private void notifyListeners() {
        for (ThreadCompleteListener listener : listeners) {
            listener.notifyOfThreadComplete(this);
        }
    }

	/**
	 *
	 */
	@Override
    public final void run() {
        try {
            doRun();
        } finally {
            notifyListeners();
        }
    }

	/**
	 *
	 */
	public abstract void doRun();
}
