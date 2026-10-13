export function createSSEController(hub) {
  return { subscribe: (req,res) => hub.subscribe(req,res) };
}
