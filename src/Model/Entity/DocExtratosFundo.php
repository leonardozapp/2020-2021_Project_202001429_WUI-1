<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * DocExtratosFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_COMPTC
 * @property int $tipo_anbima_classe_id
 * @property string $APLIC_MIN
 * @property int $QT_DIA_PAGTO_COTA
 * @property int $QT_DIA_PAGTO_RESGATE
 * @property int $PR_COTA_ETF_MAX
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 * @property \App\Model\Entity\TipoAnbimaClass $tipo_anbima_class
 */
class DocExtratosFundo extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'tipo_anbima_classe_id' => true,
        'APLIC_MIN' => true,
        'QT_DIA_PAGTO_COTA' => true,
        'QT_DIA_PAGTO_RESGATE' => true,
        'PR_COTA_ETF_MAX' => true,
        'cnpj_fundo' => true,
        'tipo_anbima_class' => true,
    ];
}
