<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * SituacaoFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property int $tipo_situacao_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_INI_SIT
 * @property \Cake\I18n\FrozenDate $DT_REG_CVM
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 * @property \App\Model\Entity\TipoSituacaoFundo $tipo_situacao_fundo
 */
class SituacaoFundo extends Entity
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
        'DT_REG_CVM' => true,
        'cnpj_fundo' => true,
        'tipo_situacao_fundo' => true,
    ];
}
