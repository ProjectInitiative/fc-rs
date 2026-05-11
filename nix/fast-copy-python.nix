{ lib, stdenv, python3, fast-copy-src, makeWrapper }:

stdenv.mkDerivation rec {
  pname = "fast-copy";
  version = "3.0.2";

  src = fast-copy-src;

  dontUnpack = true;
  dontBuild = true;

  nativeBuildInputs = [ makeWrapper ];

  installPhase = ''
    mkdir -p $out/bin
    cp ${fast-copy-src}/fast_copy.py $out/bin/.fast_copy.py
    makeWrapper ${python3}/bin/python3 $out/bin/fast-copy \
      --add-flags "$out/bin/.fast_copy.py"
  '';

  meta = with lib; {
    description = "Fast block-order copy with dedup (Python reference)";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
