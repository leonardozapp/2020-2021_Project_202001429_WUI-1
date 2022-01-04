<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * DocInfDiarioFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_COMPTC
 * @property string $VL_TOTAL
 * @property string $VL_QUOTA
 * @property string $VL_PATRIM_LIQ
 * @property string $CAPTC_DIA
 * @property string $RESG_DIA
 * @property int $NR_COTST
 * @property string|null $rentab_diaria
 * @property string $volat_diaria
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 */
class DocInfDiarioFundo extends Entity
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
        'VL_TOTAL' => true,
        'VL_QUOTA' => true,
        'VL_PATRIM_LIQ' => true,
        'CAPTC_DIA' => true,
        'RESG_DIA' => true,
        'NR_COTST' => true,
        'rentab_diaria' => true,
        'volat_diaria' => true,
        'cnpj_fundo' => true,
    ];
}
