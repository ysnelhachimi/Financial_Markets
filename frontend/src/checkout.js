// Redirige vers le prestataire de paiement en soumettant un formulaire POST
// (CMI/PayZone attend un POST de champs signés vers sa passerelle).
export function postToPaymentGateway({ payment_url, fields }) {
  const form = document.createElement("form");
  form.method = "POST";
  form.action = payment_url;
  form.style.display = "none";
  Object.entries(fields || {}).forEach(([name, value]) => {
    const input = document.createElement("input");
    input.type = "hidden";
    input.name = name;
    input.value = value;
    form.appendChild(input);
  });
  document.body.appendChild(form);
  form.submit();
}

// Formate un prix en centimes (MAD) pour l'affichage.
export function formatPrice(cents, currency = "MAD") {
  if (!cents) return "Gratuit";
  return `${(cents / 100).toLocaleString("fr-MA", { minimumFractionDigits: 0 })} ${currency}/mois`;
}
