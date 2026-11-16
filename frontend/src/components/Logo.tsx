/** FraudStream mark: two transaction "streams" with an intercepted signal node. */
export function LogoMark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 64 64" fill="none" aria-hidden="true">
      <rect width="64" height="64" rx="14" fill="#0f1422" stroke="#273049" />
      <path d="M10 22c8 0 10 8 18 8s10-8 18-8h8" stroke="#18c8dc" strokeWidth="4" strokeLinecap="round" />
      <path d="M10 34c8 0 10 8 18 8s10-8 18-8h8" stroke="#18c8dc" strokeOpacity=".45" strokeWidth="4" strokeLinecap="round" />
      <circle cx="46" cy="22" r="6" fill="#f2554a" />
      <circle cx="46" cy="22" r="10" stroke="#f2554a" strokeOpacity=".4" strokeWidth="2" />
    </svg>
  );
}

export function Logo() {
  return (
    <div className="flex items-center gap-2.5">
      <LogoMark />
      <div className="leading-tight">
        <div className="text-[15px] font-semibold tracking-[0.18em] text-mist-100">FRAUDSTREAM</div>
        <div className="text-[9.5px] tracking-[0.14em] text-mist-400 uppercase">Risk Operations Center</div>
      </div>
    </div>
  );
}
