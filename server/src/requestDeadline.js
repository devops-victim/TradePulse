// SSE applies its own 25-second connection lifetime.
export function requestDeadline(deadlineMs = 20000) {
  return (req, res, next) => {
    if (req.path === '/api/events') return next();
    const timer = setTimeout(() => {
      if (!res.headersSent) res.status(504).json({ success: false, error: 'Request deadline exceeded' });
      else res.destroy();
    }, deadlineMs);
    const clear = () => clearTimeout(timer);
    res.once('finish', clear);
    res.once('close', clear);
    next();
  };
}
