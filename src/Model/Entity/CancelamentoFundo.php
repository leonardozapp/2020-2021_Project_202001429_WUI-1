<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * CancelamentoFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_CANCEL
 * @property \Cake\I18n\FrozenDate $DT_REG_CVM
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 */
class CancelamentoFundo extends Entity
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
        'cnpj_fundo_id' => true,
        'DT_CANCEL' => true,
        'DT_REG_CVM' => true,
        'cnpj_fundo' => true,
    ];
}
