import React, { useEffect, useState } from "react";
import { useStore } from "./store";
import { getConnStatus, myClientId, onConnChange } from "./ws";
import { TopBar } from "./components/TopBar";
import { CellView } from "./components/CellView";

const App: React.FC = () => {
  const loaded = useStore((s) => s.loaded);
  const cellOrder = useStore((s) => s.cellOrder);
  const cellsById = useStore((s) => s.cells);
  const lock = useStore((s) => s.lock);
  const gens = useStore((s) => s.gens);
  const currentGenId = useStore((s) => s.currentGenId);
  const kernelAlive = useStore((s) => s.kernelAlive);
  const busy = useStore((s) =>
    [...s.executions.values()].some(
      (e) => (e.status === "running" || e.status === "queued") &&
            e.gen_id === s.currentGenId,
    ),
  );
  const [online, setOnline] = useState(getConnStatus() === "online");
  const me = myClientId();

  useEffect(() => {
    const off = onConnChange(() => setOnline(getConnStatus() === "online"));
    return () => { off(); };
  }, []);

  // 只有 held 才真正可以执行；grace 是断线宽限，不能操作
  const isHolder = lock?.state === "held" && lock.holder === me;
  const currentGenSeq = currentGenId ? gens.get(currentGenId)?.seq ?? null : null;

  if (!loaded) {
    return <div className="loading">正在加载笔记…</div>;
  }

  return (
    <div className="app">
      <TopBar
        lock={lock}
        online={online}
        currentGenSeq={currentGenSeq}
        kernelAlive={kernelAlive}
        busy={busy}
      />
      <main className="notebook">
        {cellOrder.map((id) => {
          const cell = cellsById.get(id);
          if (!cell) return null;
          return (
            <CellView
              key={id}
              cell={cell}
              lock={lock}
              isHolder={isHolder}
              online={online}
            />
          );
        })}
      </main>
      <footer className="foot">
        输出归属到具体执行（编号 #）；同单元重新运行会生成新的一次执行卡片，旧结果不会拼接。
      </footer>
    </div>
  );
};

export default App;
