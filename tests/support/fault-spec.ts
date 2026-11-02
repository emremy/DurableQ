export type FaultSpec = {
  point: string;
  action: "throw" | "busy" | "exit" | "hang" | "pause" | "none";
  /** For "busy": how many times to fail before succeeding (default: always). */
  times?: number;
  gateDir?: string;
};
