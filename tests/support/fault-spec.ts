export type FaultSpec = {
  point: string;
  action: "throw" | "exit" | "hang" | "pause" | "none";
  gateDir?: string;
};
